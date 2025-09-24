
# Установка максимального размера поля CSV
# https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
maxInt = sys.maxsize

while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 10)


class Structure:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def __str__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=2)

    def __repr__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=2)


class Connection(BaseConnectionClass):

    def __init__(self, conn_info):
        self.s3 = None
        self.conn_info = conn_info
        self.headers = {
            # OAuth-токен.
            "Authorization": "OAuth " + conn_info["password"]
        }

    def get_counter_creation_date(self, counter_id):
        '''Возвращает дату создания счетчика, для первого чекпоинта'''
        url = 'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}' \
            .format(counter_id=counter_id)

        r = requests.get(url, headers=self.headers)
        if r.status_code == 200:
            date = json.loads(r.text)['counter']['create_time'].split('T')[0]
        return date

    def get_date_period(self, checkpoint, counter_id, get_stat=None):
        '''Закачка может быть только за вчерашний день'''
        yesterday = (datetime.datetime.now(pytz.timezone('Europe/Moscow')) - datetime.timedelta(1)).strftime('%Y-%m-%d')
        if checkpoint == 'full' or checkpoint is None:
            start_date_str, end_date_str = self.get_counter_creation_date(counter_id), yesterday
        elif checkpoint == datetime.datetime.now(pytz.timezone('Europe/Moscow')).strftime(
                '%Y-%m-%d') or checkpoint == yesterday:
            start_date_str, end_date_str = yesterday, yesterday
        else:
            if get_stat:
                start_date_str = datetime.datetime.strptime(checkpoint, "%Y-%m-%d").date()
            else:
                start_date_str = (
                        datetime.datetime.strptime(checkpoint, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime(
                    '%Y-%m-%d')
            end_date_str = yesterday

        logger.info("Начальная дата запроса: {start}, конечная дата запроса: {end}".format(start=start_date_str,
                                                                                           end=end_date_str))
        return start_date_str, end_date_str

    def build_user_request(self, checkpoint, sql, counter_id):
        start_date_str, end_date_str = self.get_date_period(checkpoint, counter_id)
        # Создание структуры данных (неизменяемый кортеж) с начальным запросом пользователя
        UserRequest = namedtuple(
            "UserRequest",
            "token counter_id start_date_str end_date_str source fields"
        )

        user_request = UserRequest(
            token=self.conn_info["password"],
            counter_id=counter_id,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            source=json.loads(sql)["source"],
            fields=tuple(json.loads(sql)["fields"]),
        )

        return user_request

    def get_estimation(self, user_request):
        '''Возвращает оценку Logs API (возможность загрузки данных и максимальный период в днях)'''
        url_params = urlencode(
            [
                ('date1', user_request.start_date_str),
                ('date2', user_request.end_date_str),
                ('source', user_request.source),
                ('fields', ','.join(user_request.fields))
            ]
        )

        url = 'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequests/evaluate?' \
                  .format(counter_id=user_request.counter_id) + url_params

        for _ in range(5):
            r = requests.get(url, headers=self.headers)
            logger.info(r.text)
            if r.status_code == 200:
                return json.loads(r.text)['log_request_evaluation']
            logger.warning(r.text)
            time.sleep(5)

        r = requests.get(url, headers=self.headers)
        logger.info(r.text)
        if r.status_code == 200:
            return json.loads(r.text)['log_request_evaluation']
        else:
            raise ValueError(json.loads(r.text)['errors']['message'].encode("utf-8"))

    def get_api_requests(self, user_request):
        '''Возвращает список запросов API для UserRequest'''
        api_requests = []
        estimation = self.get_estimation(user_request)
        if estimation['possible']:
            api_request = Structure(
                user_request=user_request,
                date1_str=user_request.start_date_str,
                date2_str=user_request.end_date_str,
                status='new'
            )
            api_requests.append(api_request)
            logger.info('Кол-во запросов будет 1.Запрос 1 будет c датами {} и {}'.format(user_request.start_date_str,
                                                                                         user_request.end_date_str))
        elif estimation['max_possible_day_quantity'] != 0:
            start_date = datetime.datetime.strptime(
                user_request.start_date_str,
                '%Y-%m-%d'
            )

            end_date = datetime.datetime.strptime(
                user_request.end_date_str,
                '%Y-%m-%d'
            )

            days = (end_date - start_date).days
            num_requests = int(days / estimation['max_possible_day_quantity']) + 1
            days_in_period = int(days / num_requests) + 1
            for i in range(num_requests):
                date1 = start_date + datetime.timedelta(i * days_in_period)
                date2 = min(
                    end_date,
                    start_date + datetime.timedelta((i + 1) * days_in_period - 1)
                )

                api_request = Structure(
                    user_request=user_request,
                    date1_str=date1.strftime('%Y-%m-%d'),
                    date2_str=date2.strftime('%Y-%m-%d'),
                    status='new'
                )
                logger.info(
                    'Кол-во запросов будет {}.Запрос {} будет c датами {} и {}'.format(num_requests, i, date1, date2))
                api_requests.append(api_request)
        else:
            raise RuntimeError('Logs API не может загрузить данные: максимально возможное количество дней = 0')
        return api_requests

    def check_request(self, api_request):
        counter_id = api_request.user_request.counter_id
        url = 'https://api-metrika.yandex.net/management/v1/counter/{}/logrequests'.format(counter_id)
        r = requests.get(url, headers=self.headers)
        if r.status_code == 200:
            fields = ','.join(sorted(api_request.user_request.fields, key=lambda s: s.lower()))
            original_str = ','.join([counter_id, api_request.date1_str, api_request.date2_str, fields,
                                     api_request.user_request.source])
            for i in r.json()["requests"]:
                compare_str = ','.join(
                    [str(i['counter_id']), i['date1'], i['date2'], ','.join(i['fields']), i['source']])
                # Достаточно получить первый запрос с такими же параметрами, т.к. они выдаются в порядке создания.
                # Самый первый созданный запрос всегда первый запрос в списке.
                if original_str == compare_str:
                    api_request.status = i['status']
                    api_request.request_id = i['request_id']
                    if i['status'] == 'processed':
                        size = len(i['parts'])
                        api_request.size = size
                    return True
        else:
            return None

    def create_task(self, api_request):
        '''Создает задачу Logs API для генерации данных'''
        url_params = urlencode(
            [
                ('date1', api_request.date1_str),
                ('date2', api_request.date2_str),
                ('source', api_request.user_request.source),
                ('fields', ','.join(sorted(api_request.user_request.fields, key=lambda s: s.lower())))
            ]
        )

        url = 'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequests?' \
                  .format(counter_id=api_request.user_request.counter_id) + url_params

        r = requests.post(url, headers=self.headers)
        logger.debug(r.text)
        if r.status_code == 200:
            logger.debug(json.dumps(json.loads(r.text)['log_request'], indent=2))
            response = json.loads(r.text)['log_request']
            api_request.status = response['status']
            api_request.request_id = response['request_id']
            return response
        else:
            raise ValueError(r.text)

    def update_status(self, api_request):
        '''Возвращает статус текущих задач'''
        url = 'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequest/{request_id}' \
            .format(request_id=api_request.request_id,
                    counter_id=api_request.user_request.counter_id)

        r = requests.get(url, headers=self.headers)
        logger.debug(r.text)
        if r.status_code == 200:
            status = json.loads(r.text)['log_request']['status']
            api_request.status = status
            if status == 'processed':
                size = len(json.loads(r.text)['log_request']['parts'])
                api_request.size = size
            return api_request
        else:
            raise ValueError(r.text)

    @staticmethod
    def is_valid_date(date_str):
        try:
            parse(date_str)
            return True
        except:
            return False

    def set_checkpoint(self, source, row):
        checkpoint = row.get('ym:pv:date') if row.get('ym:s:date') is None else row.get('ym:s:date')

        if checkpoint:
            if not self.is_valid_date(checkpoint):
                logger.warning(f"Получена не правильная строка чек-поинта: {checkpoint}. Меняем на пустой значение")
                return ''
            else:
                return checkpoint
        else:
            return ''

    def save_data(self, api_request, part, path_tmp, table, num_request):
        '''Загружает часть данных из Logs API'''
        url = 'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part}/download' \
            .format(
            counter_id=api_request.user_request.counter_id,
            request_id=api_request.request_id,
            part=part
        )

        r = requests.get(url, headers=self.headers)

        if r.status_code != 200:
            logger.debug(r.text)
            raise ValueError(r.text)

        reader = csv.DictReader(io.StringIO(r.text.replace('\0', '')), delimiter='\t')
        read_data = list(reader)
        # Удаляем пустые значения.
        for csvi in range(len(read_data)):
            read_data[csvi] = {k: v for k, v in read_data[csvi].items()
                               if v != 'None' and v != 'N/A'}

        self.process_and_upload_data_to_s3(data=read_data,
                                           s3=self.s3,
                                           path_tmp=path_tmp,
                                           table=table,
                                           source='заглушка',
                                           set_checkpoint=self.set_checkpoint)

        api_request.status = 'saved'

    def clean_data(self, api_request):
        '''Очищает сгенерированные данные на сервере'''
        url = 'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequest/{request_id}/clean' \
            .format(counter_id=api_request.user_request.counter_id,
                    request_id=api_request.request_id)

        r = requests.post(url, headers=self.headers)
        logger.debug(r.text)
        if r.status_code != 200:
            raise ValueError(r.text)

        api_request.status = json.loads(r.text)['log_request']['status']
        return json.loads(r.text)['log_request']

    def integrate_with_logs_api(self, user_request, path_tmp, table, checkpoint):
        try:
            # Создаем зарос к API
            api_requests = self.get_api_requests(user_request)
            num_req = 0
            for api_request in api_requests:
                num_req += 1
                logger.debug('Проверяем на наличие готового запроса в API')
                if not self.check_request(api_request):
                    logger.debug('Создает задачу Logs API для генерации данных')
                    self.create_task(api_request)
                    logger.debug(api_request)

                delay = 20
                while api_request.status != 'processed':
                    logger.info('Пауза на %d сек.' % delay)
                    time.sleep(delay)
                    logger.info('Проверка статуса')
                    api_request = self.update_status(api_request)
                    logger.info('API Request status: ' + api_request.status)

                logger.info('Сохраняем данные')
                for part in range(api_request.size):
                    logger.info('Сохраняем часть #' + str(part) + ' из запроса #' + str(num_req))
                    self.save_data(api_request, part, path_tmp, table, num_req)

                logger.info('Чистим данные')
                self.clean_data(api_request)
        except Exception as e:
            logger.warning(e)
            raise e

    # def clean_queue(self, counter_id):
    #     url = 'https://api-metrika.yandex.net/management/v1/counter/{}/logrequests'.format(counter_id)
    #     rs = requests.get(url, headers=self.headers)
    #
    #     if rs.status_code != 200:
    #         raise ValueError('Error: '+ json.loads(rs.text)["message"])
    #
    #     url_clean = 'https://api-metrika.yandex.ru/management/v1/counter/{}/logrequest/{}/clean'
    #
    #     for req in rs.json()['requests']:
    #         u = url_clean.format(counter_id, req['request_id'])
    #         r = requests.post(u, headers=self.headers)

    # Stat API

    def create_req(self, counter_id, date1, date2, json, limit=1, offset=1):
        url = 'https://api-metrika.yandex.net/stat/v1/data?'
        try:
            filters = json['filters']
        except KeyError:
            filters = None

        params = {
            'ids': counter_id,
            'date1': date1,
            'date2': date2,
            'metrics': ','.join(json['metrics']),
            'dimensions': ','.join(json['dimensions']),
            'filters': [filters],
            'lang': "en",
            'limit': limit,
            'offset': offset,
            'accuracy': 'full',
        }

        for _ in range(5):
            r = requests.get(url, params=params, headers=self.headers)
            if r.status_code == 200:
                return r.text

            logger.info('Ошибка API')
            time.sleep(20)

        raise ValueError(r.text)

    def pages(self, total):
        offset = 1
        limit = 100000
        pages = math.ceil(total / limit)
        for page in range(pages):
            yield offset
            offset += limit

    def to_dict(self, raw_text):
        dicts = []
        text = json.loads(raw_text)

        metrics_fields = text['query']['metrics']
        dimensions_fields = text['query']['dimensions']

        for row in text['data']:
            dimensions_dict = dict(
                zip(dimensions_fields, ['' if i['name'] is None else i['name'] for i in row['dimensions']]))
            metrics_dict = dict(zip(metrics_fields, ['' if i is None else i for i in row['metrics']]))
            merged_dict = {**dimensions_dict, **metrics_dict}
            dicts.append(merged_dict)
        return dicts

    @staticmethod
    def set_checkpoint_stats_api(source, row):

        keys = ['ym:pv:date', 'ym:s:date', 'ym:ev:date', 'ym:pv:startOfWeek', 'ym:pv:startOfMonth']
        for key in row.keys():
            if key in keys:
                return row[key]
        return None



    def stats_api_executor(self, counter_id, checkpoint, sql, path_tmp, table, ):
        fields = json.loads(sql)
        dates = self.get_date_period(checkpoint, counter_id, get_stat=True)
        response = self.create_req(counter_id, *dates, fields)
        response_json = json.loads(response)
        total_rows = response_json['total_rows']
        logger.info('Строки округлены' if response_json["total_rows_rounded"] else 'Строки не округлены')
        logger.info("За период дат с {} до {} в источнике {} строк".format(*dates, str(total_rows)))

        if total_rows > 100000:
            cnt = 1
            for page_offset in self.pages(total_rows):
                data = self.create_req(counter_id, *dates, fields, 100000, page_offset)
                dicts = self.to_dict(data)
                logger.info("Кол-во строк в части {} {}".format(cnt, len(dicts)))
                self.process_and_upload_data_to_s3(data=dicts,
                                                   s3=self.s3,
                                                   path_tmp=path_tmp,
                                                   table=table,
                                                   source='заглушка',
                                                   set_checkpoint=self.set_checkpoint_stats_api)
                cnt += 1
        else:
            data = self.create_req(counter_id, *dates, fields, 100000)
            dicts = self.to_dict(data)
            self.process_and_upload_data_to_s3(data=dicts,
                                               s3=self.s3,
                                               path_tmp=path_tmp,
                                               table=table,
                                               source='заглушка',
                                               set_checkpoint=self.set_checkpoint_stats_api)

    def export_data(self, sql, path_tmp, table, checkpoint, s3=None):
        self.s3 = s3

        types_api = {
            'visits': 'logs_api',
            'hits': 'logs_api',
            'adfox': 'stats_api',
            'costs': 'stats_api',
            'isRobot': 'stats_api'
        }
        api = types_api[json.loads(sql)['source']]
        counter_id = self.conn_info["user"]

        self.path_check(path_tmp)
        # self.clean_queue(counter_id)

        if api == 'logs_api':
            user_request = self.build_user_request(checkpoint, sql, counter_id)
            self.integrate_with_logs_api(user_request, path_tmp, table, checkpoint)
        elif api == 'stats_api':
            self.stats_api_executor(counter_id, checkpoint, sql, path_tmp, table)
        else:
            raise "Неизвестный тип API указан в source файле"
        return True


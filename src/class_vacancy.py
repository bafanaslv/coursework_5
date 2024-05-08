class Vacancy:
    """ Класс для работы с вакансиями. """
    idv: int             # идентификатор вакансии
    name: str            # наименование вакансии
    area: str            # регион где находится вакансия
    requirement: str     # требования к соискателю
    responsibility: str  # долностные обязанности
    salary_min: int      # мминимальная зарплата
    salary_max: int      # максимальная зарплата
    currency: str        # валюта зарплаты
    employer_id: int     # идентификатор работодателя
    employer: str        # работодатель
    employer_url: str    # ссылка на вакансию

    max_id = 0  # максимальный идентификатор вакансии, применяется при добавлении вакансии

    def __init__(self, idv, name, area, requirement, responsibility, salary_min, salary_max,
                 currency, employer_id, employer, employer_url):
        self.idv = idv
        self.name = name
        self.area = area
        self.requirement = requirement
        self.responsibility = responsibility
        self.salary_min = salary_min
        self.salary_max = salary_max
        self.currency = currency
        self.employer_id = employer_id
        self.employer = employer
        self.employer_url = employer_url

    @classmethod
    def create_objects_vacancy(cls, hh_vacancies, vac_list, employers_list, vacancies_list) -> list:
        """Метод для создания списка объектов вакансий из списка словарей hh_vacancies полученных с HeadHanter."""
        if isinstance(hh_vacancies, dict):
            vac_id = 0
            for vacancy in hh_vacancies["items"]:
                if "id" in vacancy['employer']:
                    if len(employers_list) == 0:
                        employers_list.append([vacancy['employer']['id'],
                                               vacancy['employer']['name'], vacancy['alternate_url'], 1])
                    else:
                        list_append = False
                        for emp_list in employers_list:
                            if emp_list[0] == vacancy['employer']['id']:
                                emp_list[3] += 1
                                list_append = True
                        if list_append is False:
                            employers_list.append([vacancy['employer']['id'],
                                                   vacancy['employer']['name'], vacancy['alternate_url'], 1])

                    vac_id = int(vacancy["id"])
                    salary_min, salary_max, currency = cls.salary_valid(vacancy['salary'])
                    responsibility = cls.responsibility_valid(vacancy['snippet'])
                    vacancies_list.append([vacancy["id"], vacancy["name"], vacancy['area']['name'],
                                          vacancy['snippet']['requirement'], responsibility, salary_min, salary_max,
                                          currency, vacancy['employer']['id']])
                    vac_list.append(cls(idv=vacancy["id"],
                                          name=vacancy["name"],
                                          area=vacancy['area']['name'],
                                          requirement=vacancy['snippet']['requirement'],
                                          responsibility=responsibility,
                                          salary_min=salary_min,
                                          salary_max=salary_max,
                                          currency=currency,
                                          employer_id=vacancy['employer']['id'],
                                          employer=vacancy['employer']['name'],
                                          employer_url=vacancy['alternate_url']))
            # по мере формирование словаря vac_list выявляется максимальных номер id вакансии.
            # он нам нужен при добаволении новых вакансий для избежания повторения id.
            if vac_id > cls.max_id:
                cls.max_id = vac_id
            return vac_list
        else:
            print('Ошибочный формат файла.')

    @staticmethod
    def responsibility_valid(responsibility_item) -> str:
        """Валидация должностных обязанностей."""
        if not responsibility_item:
            return ''
        else:
            if not responsibility_item["responsibility"]:
                return 'не указан'
            else:
                return responsibility_item["responsibility"]

    @staticmethod
    def salary_valid(salary_item) -> (int, int, str):
        """Валидация зарплаты и валюты."""
        if salary_item is None:
            salary_min = 0
            salary_max = 0
            currency = ''
        else:
            if not salary_item['from']:
                salary_min = 0
            else:
                salary_min = salary_item['from']

            if not salary_item['to']:
                salary_max = 0
            else:
                salary_max = salary_item['to']

            if not salary_item['currency']:
                currency = ''
            else:
                if salary_item['currency'] == 'RUR':
                    currency = 'руб.'
                else:
                    currency = salary_item['currency']

        return salary_min, salary_max, currency

    def __str__(self) -> str:
        """Подготовка строк вакансий для вывода."""
        sal = ''
        if self.salary_min == 0 and self.salary_max == 0:
            sal = 'не указана'
        elif self.salary_min > 0 and self.salary_max > 0:
            sal = f'от {self.salary_min} до {self.salary_max}'
        elif self.salary_max > 0:
            sal = str(self.salary_max)
        elif self.salary_min > 0:
            sal = str(self.salary_min)

        return (f'ID вакансии: {self.idv}\n'
                f'Вакансия: {self.name}\n'
                f'Регион:   {self.area}\n'
                f'Требования к соискателю: {self.requirement}\n'
                f'Круг обязанностей: {self.responsibility}\n'
                f'Зарплата: {sal} {self.currency}\n'
                f'ID работодателя: {self.employer_id}\n'
                f'Работодатель: {self.employer}\n'
                f'Описание вакансии: {self.employer_url}\n')

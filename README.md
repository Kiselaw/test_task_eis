# test_task_eis
Тестовое задание на вакансию Разработчик Python. 

1. В коллекции пользователей 'Account' лежат документы вида:

{
    'number': '7800000000000',
    'name': 'Пользователь №',
    'sessions': [
        {
            'created_at': ISODate('2016-01-01T00:00:00'),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc',
            'actions': [
                {
                    'type': 'read',
                    'created_at': ISODate('2016-01-01T01:20:01'),
                },
                {
                    'type': 'read',
                    'created_at': ISODate('2016-01-01T01:21:13'),
                },
                {
                    'type': 'create',
                    'created_at': ISODate('2016-01-01T01:33:59'),
                }
            ],
        }
    ]
}

Необходимо написать агрегационный запрос, который по каждому пользователю выведет последнее действие
и общее количество для каждого из типов 'actions'. Итоговые данные должны представлять собой
список документов вида:

{
    'number': '7800000000000',
    'actions': [
        {
            'type': 'create',
            'last': 'created_at': ISODate('2016-01-01T01:33:59'),
            'count': 12,
        },
        {
            'type': 'read',
            'last': 'created_at': ISODate('2016-01-01T01:21:13'),
            'count': 12,
        },
        {
            'type': 'update',
            'last': null,
            'count': 0,
        },
        {
            'type': 'delete',
            'last': null,
            'count': 0,
        },
    ]
}

2. Есть две коллекции (таблицы) данных: accrual (долги) и payment (платежи). Обе коллекции имеют поля:

- id
- date (дата)
- month (месяц)

Необходимо написать функцию, которая сделает запрос к платежам и найдёт для каждого платежа долг, который будет им оплачен. Платёж может оплатить только долг, имеющий более раннюю дату. Один платёж может оплатить только один долг, и каждый долг может быть оплачен только одним платежом. Платёж приоритетно должен выбрать долг с совпадающим месяцем (поле month). Если такого нет, то самый старый по дате (поле date) долг.

Результатом должна быть таблица найденных соответствий, а также список платежей, которые не нашли себе долг.
Запрос можно делать к любой базе данных (mongodb, postgresql или другие) любым способом.

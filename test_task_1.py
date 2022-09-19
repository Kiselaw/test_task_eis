from pymongo import MongoClient
import datetime as dt
import pprint

client = MongoClient('mongodb://localhost:27017/')

db = client['test_task_db']

collection = db['Account']

accounts = [{
    'number': '7800000000000',
    'name': 'Пользователь №1',
    'sessions': [
        {
            'created_at': dt.datetime(2022, 9, 17, 0),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc',
            'actions': [
                {
                    'type': 'read',
                    'created_at': dt.datetime(2022, 9, 17, 1),
                },
                {
                    'type': 'read',
                    'created_at': dt.datetime(2022, 9, 17, 2),
                },
                {
                    'type': 'create',
                    'created_at': dt.datetime(2022, 9, 17, 3),
                }
            ]
        }
    ]
    },
    {
    'number': '7800000000001',
    'name': 'Пользователь №2',
    'sessions': [
        {
            'created_at': dt.datetime(2022, 9, 17, 4),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc',
            'actions': [
                {
                    'type': 'read',
                    'created_at': dt.datetime(2022, 9, 17, 5),
                },
                {
                    'type': 'read',
                    'created_at': dt.datetime(2022, 9, 17, 6),
                },
                {
                    'type': 'create',
                    'created_at': dt.datetime(2022, 9, 17, 7),
                }
            ]
        }
    ]
    }
]

# Уверен, что добавить отсутствующие действия можно как-то более "умно",
# но я работал с MongoDB в первый раз и не смог пока догадаться
# (или найти) как именно

pipeline = [
    {"$unwind": "$sessions"},
    {"$unwind": "$sessions.actions"},
    {"$group": {
        "_id": {"number": "$number", "type": "$sessions.actions.type"},
        "count": {"$sum": 1},
        "last": {"$last": "$sessions.actions.created_at"}
    }},
    {"$group": {
        "_id": "$_id.number",
        "actions": {"$push": {
            "type": "$_id.type",
            "last": "$last",
            "count": "$count"
        }
        }
    }
    },
    {"$addFields": {
        "actions": {
            "$cond": {
                "if": {"$in": ["delete", "$actions.type"]},
                "then": "$actions",
                "else": {"$concatArrays": [
                    "$actions", [
                        {
                            "type": "delete",
                            "last": "null",
                            "count": "0"
                        }
                    ]
                ]}
            },
        }
    }
    },
    {"$addFields": {
        "actions": {
            "$cond": {
                "if": {"$in": ["update", "$actions.type"]},
                "then": "$actions",
                "else": {"$concatArrays": [
                    "$actions", [
                        {
                            "type": "update",
                            "last": "null",
                            "count": "0"
                        }
                    ]
                ]}
            },
        }
    }
    },
    {"$addFields": {
        "actions": {
            "$cond": {
                "if": {"$in": ["create", "$actions.type"]},
                "then": "$actions",
                "else": {"$concatArrays": [
                    "$actions", [
                        {
                            "type": "create",
                            "last": "null",
                            "count": "0"
                        }
                    ]
                ]}
            },
        }
    }
    },
    {"$addFields": {
        "actions": {
            "$cond": {
                "if": {"$in": ["read", "$actions.type"]},
                "then": "$actions",
                "else": {"$concatArrays": [
                    "$actions", [
                        {
                            "type": "read",
                            "last": "null",
                            "count": "0"
                        }
                    ]
                ]}
            },
        }
    }
    }
]

pprint.pprint(list(db.account.aggregate(pipeline)))

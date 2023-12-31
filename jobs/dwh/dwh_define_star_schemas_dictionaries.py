dim_queries_ddl = {
    'part_information': {
        'fields': {
            'id': 'INT',
            'defaultPrice': 'FLOAT',
            'timeToProduce': 'FLOAT',
        },
        'id': 'id'
    },
    'material': {
        'fields': {
            'id': 'INT',
            'name': 'VARCHAR(255)',
        },
        'id': 'id'
    },        
    'material_prices': {
        'fields': {
            'id': 'BIGINT',
            'price': 'VARCHAR(255)',
            'date': 'DATE',
        },
        'id': 'id'
    },
    'sales': {
        'fields': {
            'contractNumber': 'INT',
            'clientName': 'VARCHAR(255)',
            'date': 'DATE',
            'cash': 'FLOAT'
        },
        'id': 'contractNumber'
    },
    'machine': {
        'fields': {
            'machineId': 'INT',
        },
        'id': 'machineId'
    },            
}

fact_query_ddl = {
    'fields': {
        'id': 'BIGINT',
        'machineId': 'INT',
        'timeOfProduction': 'datetime2(7)',
        'isDamaged': 'BIT',
        'partId': 'INT',
        'contractId': 'INT',
        'materialId': 'INT',
        'materialPriceId': 'BIGINT',
    },
    'ref': [
        {
            'table': 'part_information',
            'pk': 'id',
            'fk': 'partId'
        },
        {
            'table': 'material',
            'pk': 'id',
            'fk': 'materialId'
        }, 
        {
            'table': 'material_prices',
            'pk': 'id',
            'fk': 'materialPriceId'
        },
        {
            'table': 'sales',
            'pk': 'contractNumber',
            'fk': 'contractId'
        },
        {
            'table': 'machine',
            'pk': 'machineId',
            'fk': 'machineId'
        },
    ],
    'id': 'id',
}
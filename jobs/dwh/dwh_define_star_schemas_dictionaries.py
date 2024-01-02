dim_queries_ddl = {
    'part_information': {
        'fields': {
            'id': 'INT',
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
            'date': 'DATE',
        },
        'id': 'id'
    },
    'client': {
        'fields': {
            'id': 'INT',
            'clientName': 'VARCHAR(255)',
        },
        'id': 'id'
    },
    'machine': {
        'fields': {
            'id': 'INT',
        },
        'id': 'id'
    },
    'unit': {
        'fields': {
            'id': 'BIGINT',
            'timeOfProduction': 'datetime2(7)'
        },
        'id': 'id'
    },
    'time': {
        'fields': {
            'id': 'INT',
            'year': 'SMALLINT',
            'month': 'TINYINT',
            'day': 'TINYINT',
            'semester': 'TINYINT',
            'quarter': 'TINYINT',
        },
        'id': 'id'
    }            
}

fact_queries_ddl = {
    'supply_chain': {    
        'fields': {
            'machineId': 'INT',
            'partId': 'INT',
            'unitId': 'BIGINT',
            'materialId': 'INT',
            'materialPriceId': 'BIGINT',
            'isDamaged': 'BIT',
            'partDefaultPrice': 'FLOAT',
            'materialPrice': 'FLOAT',
        },
        'cluster': {
            'pk': ['machineId', 'partId', 'unitId', 'materialId' ,'materialPriceId',],
            'constraint': 'PK_FACT_SUPPLY_CHAIN_INTEGRITY'
        }
    },
   'sales': {    
        'fields': {
            'partId': 'INT',
            'clientId': 'INT',
            'cash': 'FLOAT',
        },
        'cluster': {
            'pk': ['partId', 'clientId',],
            'constraint': 'PK_FACT_SALES_INTEGRITY'
        }
    },
}
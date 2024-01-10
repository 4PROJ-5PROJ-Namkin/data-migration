import os

dim_queries_ddl = {
    'part_information': {
        'fields': {
            **({'trscPartId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
            'partId': 'INT',
            'timeToProduce': 'FLOAT',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'partId'
    },
    'material': {
        'fields': {
            **({'trscMaterialId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            'materialId': 'INT',
            'name': 'VARCHAR(255)',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'materialId'
    },        
    'contract': {
        'fields': {
            **({'trscContractId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            'contractId': 'INT',
            'clientName': 'VARCHAR(255)',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'contractId'
    },
    'machine': {
        'fields': {
            **({'trscMachineId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            'machineId': 'INT',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'machineId'
    },
    'time': {
        'fields': {
            'timeId': 'INT',
            'date': 'DATE',
            'year': 'SMALLINT',
            'month': 'TINYINT',
            'day': 'TINYINT',
            'semester': 'TINYINT',
            'quarter': 'TINYINT',
        },
        'id': 'timeId'
    }            
}

fact_queries_ddl = {
    'supply_chain': {    
        'fields': {
            **({'trscMachineId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
            **({'trscPartId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            **({'trscMaterialId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),                        
            'machineId': 'INT',
            'partId': 'INT',
            'materialId': 'INT',
            'timeOfProduction': 'DATE',
            'materialPrice': 'FLOAT',
            'timeId': 'INT',
            'materialDate': 'DATE',
            'partDefaultPrice': 'FLOAT',
            'isDamaged': 'BIT',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'cluster': {
            'pk': ['machineId', 'partId', 'unitId', 'materialId' ,'materialPriceId',],
            'constraint': 'PK_FACT_SUPPLY_CHAIN_INTEGRITY'
        } if 'DWH' in os.getenv('DB_NAME').upper() else {}
    },
   'sales': {    
        'fields': {
            **({'trscPartId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            **({'trscContractId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),                        
            'partId': 'INT',
            'contractId': 'INT',
            'cash': 'FLOAT',
            'date': 'DATE',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'cluster': {
            'pk': ['partId', 'clientId',],
            'constraint': 'PK_FACT_SALES_INTEGRITY'
        } if 'DWH' in os.getenv('DB_NAME').upper() else {}
    },
}
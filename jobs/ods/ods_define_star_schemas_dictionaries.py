import os
from dotenv import load_dotenv
load_dotenv('../../.env')

dim_queries_ddl = {
    'part_information': {
        'fields': {
            **({'trscPartId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
            'partId': 'INT',
            'timeToProduce': 'FLOAT',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'partId' if 'DWH' in os.getenv('DB_NAME').upper() else {}
    },
    'material': {
        'fields': {
            **({'trscMaterialId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            'materialId': 'INT',
            'name': 'VARCHAR(255)',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'materialId' if 'DWH' in os.getenv('DB_NAME').upper() else {}
    },        
    'contract': {
        'fields': {
            **({'trscContractId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            'contractId': 'INT',
            'clientName': 'VARCHAR(255)',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'contractId' if 'DWH' in os.getenv('DB_NAME').upper() else {}
    },
    'machine': {
        'fields': {
            **({'trscMachineId': 'INT DEFAULT NULL'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),            
            'machineId': 'INT',
            **({'lastUpdate': 'datetime2(7)'} if 'ODS' in os.getenv('DB_NAME').upper() else {}),
        },
        'id': 'machineId' if 'DWH' in os.getenv('DB_NAME').upper() else {}
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
        'id': 'timeId' if 'DWH' in os.getenv('DB_NAME').upper() else {}
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
            'materialPriceDate': 'DATE',
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
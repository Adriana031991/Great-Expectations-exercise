import datetime
import pandas as pd

import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.checkpoint import LegacyCheckpoint

def great_expectations():
    context = ge.data_context.DataContext()
    expectation_suite_name = "testdata"
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    batch_kwargs = {'data_asset_name': 'data', 'datasource': 'data', 'path': 'D:\\dev\\ejercicios-varios\\Great-Expectations-exercise\\data\\data.csv', 'reader_method': 'read_csv'}
    batch = context.get_batch(batch_kwargs, suite)

    puede = batch.expect_column_values_to_match_regex('palabra', '[P,p][U,u][E,e][D,d]\w{1,}', mostly=0.01 )
    countPuede = puede["result"]["element_count"]-puede["result"]["unexpected_count"]
    esperada = 1
    es = esperada.__eq__(countPuede)
    print("el valor ", countPuede, "es igual a el valor", esperada, "?: ", es)
    # print(es)

    dicResult = batch.expect_column_values_to_match_regex('palabra', '[E,e][M,m][P,p][A,a][N,n][I,i][Z,z]\w{1,}', mostly=0.06)
    countEmpaniz = dicResult["result"]["element_count"]-dicResult["result"]["unexpected_count"]
    esperado = 5
    er = esperado.__eq__(countEmpaniz)
    print("el valor ", countPuede, "es igual a el valor", esperada, "?: ", er)
    # print(er)


    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
                "batch_kwargs": batch_kwargs,
                "expectation_suite_names": [expectation_suite_name]
            }
        ]
    ).run()

def read_data():
    data = pd.read_csv('./data/data.csv')
    word = data['palabra'] == 'Empanizados'
    filtered_data = data[word]
    filtered_data = filtered_data['palabra']
    return filtered_data


if __name__ == '__main__':
   great_expectations()
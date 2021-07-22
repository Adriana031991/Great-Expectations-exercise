import datetime
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.checkpoint import LegacyCheckpoint

context = ge.data_context.DataContext()

expectation_suite_name = "testdata"

suite = context.get_expectation_suite(expectation_suite_name)
suite.expectations = []

batch_kwargs = {'data_asset_name': 'data', 'datasource': 'data', 'path': 'D:\\dev\\ejercicios-varios\\GE\\data\\data.csv', 'reader_method': 'read_csv'}
batch = context.get_batch(batch_kwargs, suite)

puede = batch.expect_column_values_to_match_regex('palabra', '[P,p][U,u][E,e][D,d]\w{1,}', mostly=0.01 )
countPuede = puede["result"]["element_count"]-puede["result"]["unexpected_count"]
print(countPuede)

dicResult = batch.expect_column_values_to_match_regex('palabra', '[E,e][M,m][P,p][A,a][N,n][I,i][Z,z]\w{1,}', mostly=0.06)
countEmpaniz = dicResult["result"]["element_count"]-dicResult["result"]["unexpected_count"]
print(countEmpaniz)


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

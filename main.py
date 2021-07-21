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

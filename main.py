import json
import datetime
import argparse
import apache_beam as beam

from typing import Tuple, List, Dict
from apache_beam.options.pipeline_options import PipelineOptions


class IoOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", type = str, help = "Input files wildcard", dest = "input_path", required = True)
        parser.add_argument("--output", type = str, help = "Output file preffix", dest = "output_path", required = True)


def str_to_timestamp(string_date: str) -> int:
    """ Transforms a string date in the YYYY-mm-dd HH:MM:SS format to timestamp"""
    return datetime.datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S").timestamp()

def select_keys(record: Dict[str, str], keys: List[str]) -> Dict[str, str]:
    """ Return a subset of the input dictionary with only the specified keys"""
    return { key: record[key] for key in keys }


def run(pipeline_options: PipelineOptions) -> None:

    io_options = pipeline_options.view_as(IoOptions)

    with beam.Pipeline(options = pipeline_options) as pipeline: 

        user_data = \
            (pipeline 
                | "ReadData"  >> beam.io.ReadFromText(io_options.input_path) #Read All Files
                | "ParseJson" >> beam.Map(json.loads)) #Parse json line as a Python dict 
        
        found_custumers = \
            (user_data
                | "SetTimestamp"   >> beam.Map(lambda e: beam.window.TimestampedValue(e, str_to_timestamp(e['timestamp'])))
                | "Windowing"      >> beam.WindowInto(beam.window.Sessions(60 * 10)) #Create a 600 seconds (10 minutes) session for every page visit
                | "Reshape"        >> beam.Map(lambda e: (e['customer'], e['page'])) #Define the key as the customer_id and value as the page type
                | "GroupBy"        >> beam.GroupByKey() #Group by customer_id and overlaping sessions, collecting the page visits in the mean time
                | "FilterCheckout" >> beam.Filter(lambda e: 'checkout' not in e[1]) #Get the customers that haven't oppened the checkout page in the accumulated sessions
                | "RemoveWindow"   >> beam.WindowInto(beam.window.GlobalWindows()) #Remove the time session to use the values as a filter later
                | "SelectCustomer" >> beam.Map(lambda e: e[0])) #Select the customers ids 

        customer_product = \
            (user_data
                | "FindProducts"    >> beam.Filter(lambda e: 'product' in e.keys()) #Get the records that have the 'product' key
                | "SelectKeys"      >> beam.Map(lambda e: select_keys(e, ['timestamp', 'customer', 'product'])) #select the timestamp, customer and product values
                | "FilterCustomers" >> beam.Filter(lambda e, target_customers: e['customer'] in target_customers, 
                                                   target_customers = beam.pvalue.AsIter(found_custumers)) #Get the records of the customers that abandonned the basket
                | "SetCustomerKey"  >> beam.WithKeys(lambda e: e['customer'])
                | "GroupByCustomer" >> beam.GroupByKey() #Collect the customers records
                | "GetLastAccess"   >> beam.Map(lambda e: e[1][-1]) #Get the customers last access
                | "FormatAsJson"    >> beam.Map(lambda e: json.dumps(e)) 
                | "WriteOutput"     >> beam.io.WriteToText(io_options.output_path, file_name_suffix = '.json')) 


if __name__ == '__main__':

    pipeline_options = PipelineOptions()
 
    run(pipeline_options)

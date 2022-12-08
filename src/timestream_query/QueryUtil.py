#!/usr/bin/python
import sys
import traceback


class QueryUtil:
    def __init__(self, client, database_name, table_name, disable_data_print=False):
        self.client = client
        self.paginator = client.get_paginator("query")
        self.disable_data_print = disable_data_print

        # See records ingested into this table so far
        self.SELECT_ALL = "SELECT * FROM " + database_name + "." + table_name

    def run_simple_select_all_query(self):
        # This query can potentially be expensive. Only use this if you know what you are doing
        self.run_query(self.SELECT_ALL)

    def run_query(self, query_string, obj=False):
        try:
            page_iterator = self.paginator.paginate(QueryString=query_string)
            data = []
            for page in page_iterator:
                results = self.__parse_query_result(page, obj)
                if obj == True:
                    data.extend(results)

            if obj == True:
                return data
        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)

    def __parse_query_result(self, query_result, obj=False):
        column_info = query_result["ColumnInfo"]

        # print("Metadata: %s" % column_info)
        if not self.disable_data_print:
            print("Data: ")
        data = []
        for row in query_result["Rows"]:
            if obj == True:
                data.append(self.__parse_row(column_info, row, True))
            else:
                print(self.__parse_row(column_info, row))

        if obj == True:
            return data

    def __parse_row(self, column_info, row, obj=False):
        data = row["Data"]
        row_output = []
        row_output_obj = {}
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(self.__parse_datum(info, datum))
            row_output_obj[info["Name"]] = self.__parse_datum_obj(info, datum)

        if obj == True:
            return row_output_obj

        return "{%s}" % str(row_output)

    # currently only works with NullValue and ScalarValues
    def __parse_datum_obj(self, info, datum):
        if datum.get("NullValue", False):
            return None

        column_type = info["Type"]

        if column_type["ScalarType"] == "BIGINT":
            return int(datum["ScalarValue"])
        elif column_type["ScalarType"] == "DOUBLE":
            return float(datum["ScalarValue"])
        elif column_type["ScalarType"] == "TIMESTAMP":
            return datum["ScalarValue"]
        elif column_type["ScalarType"] == "VARCHAR":
            return datum["ScalarValue"]
        else:
            return None

    def __parse_datum(self, info, datum):
        if datum.get("NullValue", False):
            return "%s=NULL" % info["Name"]

        column_type = info["Type"]

        # If the column is of TimeSeries Type
        if "TimeSeriesMeasureValueColumnInfo" in column_type:
            return self.__parse_time_series(info, datum)

        # If the column is of Array Type
        elif "ArrayColumnInfo" in column_type:
            array_values = datum["ArrayValue"]
            return "%s=%s" % (
                info["Name"],
                self.__parse_array(info["Type"]["ArrayColumnInfo"], array_values),
            )

        # If the column is of Row Type
        elif "RowColumnInfo" in column_type:
            row_column_info = info["Type"]["RowColumnInfo"]
            row_values = datum["RowValue"]
            return self.__parse_row(row_column_info, row_values)

        # If the column is of Scalar Type
        else:
            return self.__parse_column_name(info) + datum["ScalarValue"]

    def __parse_time_series(self, info, datum):
        time_series_output = [
            f"{{time={data_point['Time']}, value={self.__parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],data_point['Value'])}}}"
            for data_point in datum["TimeSeriesValue"]
        ]

        return f"[{time_series_output}]"

    def __parse_column_name(self, info):
        if "Name" in info:
            return info["Name"] + "="
        else:
            return ""

    def __parse_array(self, array_column_info, array_values):
        array_output = [
            f"{self.__parse_datum(array_column_info, datum)}" for datum in array_values
        ]

        return f"[{array_output}]"

    def run_query_with_multiple_pages(self, limit=None, obj=False):
        query_with_limit = self.SELECT_ALL
        if limit is not None:
            query_with_limit += " LIMIT " + str(limit)
        print("Starting query with multiple pages : " + query_with_limit)
        self.run_query(query_with_limit, obj)

    def cancel_query(self):
        print("Starting query: " + self.SELECT_ALL)
        result = self.client.query(QueryString=self.SELECT_ALL)
        print("Cancelling query: " + self.SELECT_ALL)
        try:
            self.client.cancel_query(QueryId=result["QueryId"])
            print("Query has been successfully cancelled")
        except Exception as err:
            print("Cancelling query failed:", err)

from decimal import Decimal
import boto3


class DynamoDBAPI():
    client = None
    table = None
    table_name = ''
    key = {'partition_key': ''}

    def __init__(self, table_name, partition_key, sort_key):
        self.client_load()
        self.table_load(table_name)
        self.key['partition_key'] = partition_key
        if sort_key != '':
            self.key['sort_key'] = sort_key

    def _on_error(self, error=''):
        if error != '':
            print(error)
        self.unload()

    def __repr__(self):
        return f'<DynamoDB:{self.table_name}>'

    def table_loaded(self):
        return self.table is not None

    def connected(self):
        return self.client is not None

    def client_load(self):
        self.client = boto3.resource('dynamodb')

    def unload(self):
        self.client = None

    def table_load(self, table_name):
        self.table = self.client.Table(table_name)

    def write_rows(self, rows, decimal_precision=2):
        row_count = len(rows)
        if row_count > 0:
            try:
                with self.table.batch_writer() as batch:
                    for row in rows:
                        for f in row:
                            if isinstance(row[f], float):
                                row[f] = Decimal(str(round(row[f], decimal_precision)))
                        batch.put_item(Item=row)
            except Exception as e:
                self._on_error(str(e))

    def scan(self):
        items = []
        keys = []
        start_key = 'start'
        batch_max = 50
        batch = 0
        while start_key != {} and batch <= batch_max:
            if start_key == 'start':
                response = self.table.scan()
            else:
                response = self.table.scan(ExclusiveStartKey=start_key)
            if 'Items' in response:
                items = items + response['Items']
                if 'sort_key' in self.keys():
                    keys = keys + [{
                        self.keys['partition_key']: i[self.keys['partition_key']],
                        self.keys['sort_key']: i[self.keys['sort_key']]
                    } for i in response['Items']
                    ]
                else:
                    keys = keys + [{
                        self.keys['partition_key']: i[self.keys['partition_key']]
                    } for i in response['Items']
                    ]

            start_key = {}
            batch = batch + 1
            if 'LastEvaluatedKey' in response:
                start_key = response['LastEvaluatedKey']
        print(f'scan completed in {batch} batches out of max {batch_max}')
        return keys, items

    def delete_items(self, keys):
        if len(keys) > 0:
            try:
                with self.table.batch_writer() as batch:
                    for key in keys:
                        batch.delete_item(Key=key)
            except Exception as e:
                self._on_error(str(e))

    def delete_all(self):
        keys, items = self.scan()
        self.delete_items(keys)
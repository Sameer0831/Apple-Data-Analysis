class DataSource:
    '''
    Abstract Class, to implement factory pattern
    '''
    def __init__(self, path):
        self.path = path
    
    def create_dataframe(self):
        '''
        Abstract method, It will be implemented at the subclasses itself
        '''
        raise ValueError("Method not implemented")

class CSVDataSource(DataSource):
    ''' NO need of the init method, we will get it from abstract class (inheritance) '''
    def create_dataframe(self):
        '''
        Implementing the abstract method for the CSV DataSource
        '''
        return (
            spark
            .read
            .format('csv')
            .option('header', True)
            .load(self.path) # Reads the path as parameter
        )

class PARQUETDataSource(DataSource):
    ''' NO need of the init method, we will get it from abstract class (inheritance) '''
    def create_dataframe(self):
        '''
        Implementing the abstract method for the Parquet DataSource
        '''
        return (
            spark
            .read
            .format('parquet')
            .load(self.path) # Reads the path as parameter
        )

class DELTADataSource(DataSource):
    ''' NO need of the init method, we will get it from abstract class (inheritance) '''
    def create_dataframe(self):
        '''
        Implementing the abstract method for the Delta DataSource
        '''
        table_name = self.path
        return (
            spark
            .read
            .table(table_name) # Reads the table_name as parameter
        )

def get_data_source(data_type, filepath):
    if data_type == 'csv':
        return CSVDataSource(filepath)
    elif data_type == 'parquet':
        return PARQUETDataSource(filepath)
    elif data_type == 'delta':
        return DELTADataSource(filepath)
    else:
        raise ValueError(f"Not implemented for data_type: {data_type}")

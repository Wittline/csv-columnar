import concurrent.futures
import os
import gzip
import mmap
import concurrent.futures


class csvColumnar(object):

    def __init__(self, filepath, prefix, header = True, sep = ','):
        self.filepath = filepath
        self.header = header
        self.prefix = prefix
        self.folder = os.path.splitext(self.filepath)[0]
        self.sep = sep


    def __estimate_csv_rows(self, filename, header = True):

        count_rows = 0

        with open(filename, mode="r", encoding = "ISO-8859-1") as file_obj:

            with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as map_file:

                buffer = map_file.read(1<<13)
                file_size = os.path.getsize(filename)
                count_rows = file_size // (len(buffer) // buffer.count(b'\n')) - (1 if header else 0) 
        
        return count_rows

    
    def __clear_column_data(self, columns_names):
        return { column:[] for i, column in enumerate(columns_names)}


    def __split_process(self, filepath, chunk_size = 10):
                
        row_number = 0
        with open(filepath, 'r', encoding='utf-8-sig') as f:

            first_line = next(f)            
            columns_names = first_line.strip().split(',')            

            columns_data = self.__clear_column_data(columns_names)

            for line in f:
                row = line.strip().split(self.sep)
                for i, cell in enumerate(row):
                    columns_data[columns_names[i]].append(cell + '\n')
                
                row_number += 1                            
                
                if row_number == chunk_size:
                    yield columns_data
                    row_number = 0
                    columns_data = self.__clear_column_data(columns_names)              

        if row_number > 0:
            yield columns_data

    

    def to_columnar(self, batches = 10, compress = False):

        chunk_size = (self.__estimate_csv_rows(self.filepath, self.header) // batches)                     
        files = {}
        format = 'csv.gz' if compress else 'csv'
        
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

        for chunk in self.__split_process(self.filepath, chunk_size):

            with concurrent.futures.ThreadPoolExecutor() as executor:

                tasks = []
                for value in chunk:
                    
                    t_filepath = os.path.join(self.folder, f'{self.prefix}_{value}.{format}')
                    
                    if t_filepath not in files:
                        rows = chunk[value]
                        files[t_filepath] = open(t_filepath, 'a') 
                                
                    def write_rows(file, rows):
                        try:         
                            file.writelines(rows)
                            file.flush()
                        except Exception as er:
                            print(er)
                    
                    tasks.append(executor.submit(write_rows, files[t_filepath], rows))
                
                concurrent.futures.wait(tasks)


csvspl = csvColumnar('csvData.csv', '', header = True, sep=',')
csvspl.to_columnar(batches= 10, compress=False)
        
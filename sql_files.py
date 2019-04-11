import os


class SqlFiles(object):
    ext = 'sql'

    def __init__(self, basedir):
        self.dir_path = os.path.join(basedir, 'sql_select_queries')

    def get_files(self):
        sql_files = []
        for root, dirs, files in os.walk(self.dir_path):
            sql_files.extend([os.path.join(root, f) for f in files if f.endswith('.' + self.ext)])

        return sql_files

    def get_sqls(self):
        files = self.get_files()

        return [Sql(f) for f in files]


class Sql(object):
    __sql = None
    file_path = None
    column_name = None
    store_type = None

    def __init__(self, file_path):
        self.file_path = file_path
        file_name = file_path.split('/')[-1]
        dot_splitted_file_name = file_name.split('.')
        self.column_name = dot_splitted_file_name[0]
        if len(dot_splitted_file_name) > 2:
            self.store_type = dot_splitted_file_name[-2]

    @property
    def sql(self):
        if not self.__sql:
            with open(self.file_path, 'r') as f:
                self.__sql = f.read()

        return self.__sql

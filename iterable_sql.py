from datetime import datetime as dt

import torch
from sqlalchemy import column, create_engine, lambda_stmt, select, text
from torch.utils.data import IterableDataset


class SQLIterableDataset(IterableDataset):
    def __init__(self, conn_str, table, iterable_column,
                 iterate_from=None, iterate_to=None,
                 columns=None, dt_to_str=True):
        super(SQLIterableDataset).__init__()
        self.table = table
        self.iterable_column = iterable_column
        self.iterate_from = iterate_from
        self.iterate_to = iterate_to
        self.dt_to_str = dt_to_str
        self.cols = columns
        self.engine = create_engine(conn_str)
        self.connection = self.engine.connect()
        self.connection.execution_options(autocommit=True)
        self.data, self.columns = self.get_data()

    def __iter__(self):
        return self.data.__iter__()

    def __next__(self):
        return self.data.__next__()._asdict()

    def get_data(self):
        c = ','.join(self.cols) if self.cols else '*'
        query = text(f' {c} from {self.table}')
        stmt = lambda_stmt(lambda: select(query))
        if self.iterate_from and self.iterate_to:
            col = column(self.iterable_column)
            it_from = self.iterate_from
            it_to = self.iterate_to
            stmt += lambda s: s.where((col >= it_from) & (col <= it_to))
        if self.iterable_column:
            col = column(self.iterable_column)
            stmt += lambda s: s.order_by(col)
        result = self.connection.execute(stmt)
        return result, [col for col in result.keys()]

    def __del__(self):
        self.connection.close()
        self.data.close()
        self.engine.dispose()

    @staticmethod
    def sqlcollate(batch):
        res = []
        for a in batch:
            res_d = {x: str(y) if type(y) == dt else y for x, y in a.items()}
            res.append(res_d)
        if len(batch) == 0:
            return torch.utils.data.dataloader.default_collate(list())
        return torch.utils.data.dataloader.default_collate(res)

# **IterableDataset for pytorch from SQL Table**

This is the implementation of torch.utils.data.IterableDataset that iterates over rows from SQL Table.<br/>
Fully compatible with torch.utils.data.DataLoader<br/>
Class includes collate_fn function to convert sql types to python.

# Parameters
## Reqiured:
- conn_str (SQLAlchemy connection string)
- table (table name)
- iterable_column (column_to_iterate_over)
## Optional:
- iterate_from (value to start iteration over iterable_column)
- iterate_to (value to end iteration over iterable_column)
- columns (list of columns to include in query)

# Quick start
```python
args = {
    'conn_str': '<SQLAlchemy_connection_string>',
    'table': '<your_table_name>',
    'iterable_column': '<column_to_iterate_over>'
       }
dataset = MSSQLIterableDataset(**args)
dataloader = DataLoader(dataset, batch_size=3000,
                        shuffle=False, drop_last=False,
                        collate_fn=dataset.sqlcollate)
```

### More examples at [Example notebook](https://github.com/Greatfess/iterable_dataset_sql/blob/master/SQLIterableDataset_examples.ipynb)
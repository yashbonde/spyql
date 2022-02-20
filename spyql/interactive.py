import os

from .cli import clean_query, parse, file_ext2type
from .processor import Processor
from .writer import Writer
from .log import *

class Q:
  def __init__(self, query: str) -> None:
    """
    Make spyql interactive.

    [ IMPORT python_module [ AS identifier ] [, ...] ]
    SELECT [ DISTINCT | PARTIALS ]
        [ * | python_expression [ AS output_column_name ] [, ...] ]
        [ FROM csv | spy | text | python_expression | json [ EXPLODE path ] ]
        [ WHERE python_expression ]
        [ GROUP BY output_column_number | python_expression  [, ...] ]
        [ ORDER BY output_column_number | python_expression
            [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
        [ LIMIT row_count ]
        [ OFFSET num_rows_to_skip ]
        [ TO csv | json | spy | sql | pretty | plot ]

    Usage
    -----

    .. code-block:: python

      >>> q = Q("IMPORT numpy SELECT numpy.mean(data->salary) FROM data WHERE data->name == 'akash'")
      >>> q(data = data)

    Args
    ----

      query(str): SpyQL string
      input_opt/output_opt: kwargs for the input and writers, in this case of interactive mode we can
        ignore these
    """
    self.query = query
    self.parsed, self.strings = parse(clean_query(query))
    self.output_path = None

    # from logic can handle list/tuple and files
    _from = self.parsed["from"]
    input_options = {}
    interactive = False
    if _from == None:
      # SELECT 1
      pass
    elif os.path.exists(_from):
      # SELECT * FROM /tmp/spyql.jsonl
      ext = _from.split(".")[-1].lower()
      processor_from = file_ext2type.get(ext, None)
      if processor_from == None:
        user_warning(f"Unsupported file extension: '{ext}', loading as 'TEXT'")
        processor_from = "TEXT"

      self.parsed["from"] = processor_from
      interactive = True
      input_options = {"filepath": _from}
    else:
      # SELECT * FROM data
      interactive = True
      input_options = {"source": _from}

    _to = self.parsed["to"]
    if _to == None:
      self.parsed["to"] = "PYTHON" # force return to python
    elif isinstance(_to, str):
      if _to.upper() in Writer._valid_writers:
        raise SyntaxError(f"Cannot export to a writer format in interactive mode: '{_to}'")
      writer = Writer._ext2filetype.get(_to.split(".")[-1].lower(), None)
      if writer == None:
        raise SyntaxError(f"Invalid TO statement: '{_to}'")
      self.parsed["to"] = writer
      self.output_path = _to
    else:
      raise SyntaxError(f"Unsupported output type: '{_to}'")

    try:
      self.processor = Processor.make_processor(
        prs = self.parsed,
        strings = self.strings,
        interactive = interactive,
        input_options = input_options
      )
    except Exception as e:
      user_error(f"Failed to parse query: '{query}'", e)

  def __repr__(self) -> str:
    return f"Q(\"{self.query}\")"

  def __call__(self, **kwargs):
    # kwargs can take in multiple data sources as input in the future
    if self.output_path != None:
      with open(self.output_path, "w") as f:
        self.processor.go(
          output_file = f,
          output_options = {},
          user_query_vars = kwargs
        )
    else:
      out = self.processor.go(
        output_file = None,
        output_options = {},
        user_query_vars = kwargs
      )
      return out.output

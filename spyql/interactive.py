from fileinput import close
import os
import sys
import io
import logging
from .parser import parse
from .processor import Processor
from .writer import Writer
import spyql.log


class Query:
    def __init__(
        self,
        query: str,
        input_options: dict = {},
        output_options: dict = {},
        unbuffered=False,
        warning_flag="default",
        verbose=0,
    ) -> None:
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

        logging.basicConfig(level=(3 - verbose) * 10, format="%(message)s")
        spyql.log.error_on_warning = warning_flag == "error"

        self.query = query
        self.parsed, self.strings = parse(query)
        self.output_file = None
        self.output_options = output_options
        self.input_file = None
        self.input_options = input_options
        self.unbuffered = unbuffered

        spyql.log.user_debug_dict("Parsed query", self.parsed)
        spyql.log.user_debug_dict("Strings", self.strings.strings)

        # FROM logic:
        #   if nothing then it might be just a SELECT method
        #   if such a path exists then load the correct writer
        #   else assume it is a python object to be loaded by user
        _from = self.parsed["from"]
        if _from and isinstance(_from, str) and os.path.exists(_from):
            # SELECT * FROM /tmp/spyql.jsonl
            processor = Processor._ext2filetype.get(_from.split(".")[-1].lower(), None)
            if not processor:
                raise SyntaxError(f"Invalid FROM statement: '{_from}'")

            self.parsed["from"] = processor
            self.input_file = open(_from, "r")

        # TO logic:
        #   if nothing is determined meaning return
        #   if is a string
        #     if is a filepath -> write to file
        _to = self.parsed["to"]
        if not _to:
            self.parsed["to"] = "PYTHON"  # force return to python
        elif _to.upper() in Writer._valid_writers:
            self.parsed["to"] = _to
        elif isinstance(_to, str):
            # TO /tmp/spyql.jsonl
            writer = Writer._ext2filetype.get(_to.split(".")[-1].lower(), None)
            if writer == None:
                raise SyntaxError(f"Invalid TO file: '{_to}'")
            self.parsed["to"] = writer
            self.output_file = open(_to, "w")
        else:
            raise SyntaxError(
                f"Unsupported output type: '{_to}', {Writer._valid_writers}"
            )

        # make the processor
        self.processor = Processor.make_processor(
            self.parsed,
            self.strings,
            self.input_file if self.input_file else sys.stdin,
            self.input_options,
        )

    def __repr__(self) -> str:
        return f'Q("{self.query}")'

    def __call__(self, **kwargs):
        # kwargs can take in multiple data sources as input in the future
        fout = self.output_file if self.output_file else sys.stdout
        if self.unbuffered:
            fout = io.TextIOWrapper(open(fout.fileno(), "wb", 0), write_through=True)

        out = None
        try:
            out = self.processor.go(
                fout,
                output_options=self.output_options,
                user_query_vars=kwargs,
            )
        finally:
            if self.input_file:
                self.input_file.close()
            if self.output_file:
                self.output_file.close()
            if out:
                return out.get("output")

import csv

from django.core.management.base import BaseCommand, CommandError

from etl.forms import VirusForm


class Command(BaseCommand):
    help = ("HELP")

    def add_arguments(self, parser):
        parser.add_argument("file_path", nargs=1, type=str)

    def handle(self, *args, **options):
        self.file_path = options["file_path"][0]
        self.prepare()
        self.main()
        self.finalise()

    def prepare(self):
        self.imported_counter = 0
        self.skipped_counter = 0

    def main(self):
        self.stdout.write("Import COVID")

        with open(self.file_path, mode="r") as f:
            reader = csv.DictReader(f)
            for index, row_dict in enumerate(reader):
                form = VirusForm(data=row_dict)
                if form.is_valid():
                    form.save()
                    self.imported_counter += 1
                else:
                    self.stderr.write(f"Errors import COVID")
                                      #f"{row_dict['dateRep']} - {row_dict['cases']}:\n")
                    self.stderr.write(f"{form.errors.as_json()}\n")
                    self.skipped_counter += 1

    def finalise(self):
        self.stderr.write(f"----------------\n")
        self.stderr.write(f"Covid imported: {self.imported_counter}\n")
        self.stderr.write(f"Covid skipped: {self.skipped_counter}\n")

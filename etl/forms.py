from django import forms
from .models import CsvData

class CsvDataForm(forms.ModelForm):
    class Meta:
        model = CsvData
        fields = "__all__"



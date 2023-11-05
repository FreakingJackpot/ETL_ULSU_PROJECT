from django import forms
from .models import Virus

class VirusForm(forms.ModelForm):
    class Meta:
        model = Virus
        fields = "__all__"



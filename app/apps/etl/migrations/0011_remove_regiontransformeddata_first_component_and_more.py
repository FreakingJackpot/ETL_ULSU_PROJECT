# Generated by Django 4.2.6 on 2023-12-24 08:32

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('etl', '0010_rename_full_vaccinated_gogovglobaldata_second_component'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='regiontransformeddata',
            name='first_component',
        ),
        migrations.RemoveField(
            model_name='regiontransformeddata',
            name='second_component',
        ),
        migrations.RemoveField(
            model_name='regiontransformeddata',
            name='vaccinations_population_ratio',
        ),
        migrations.RemoveField(
            model_name='regiontransformeddata',
            name='weekly_vaccinations_infected_ratio',
        ),
    ]
# Generated by Django 4.2.6 on 2024-01-02 20:32

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('etl', '0012_region'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='gogovglobaldata',
            name='children_vaccinated',
        ),
        migrations.RemoveField(
            model_name='gogovglobaldata',
            name='need_revaccination',
        ),
        migrations.RemoveField(
            model_name='gogovglobaldata',
            name='revaccinated',
        ),
        migrations.RenameModel(
            old_name='gogovglobaldata',
            new_name='gogovdata',

        )
    ]
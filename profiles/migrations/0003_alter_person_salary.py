# Generated by Django 4.2.7 on 2023-11-28 18:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('profiles', '0002_alter_person_table'),
    ]

    operations = [
        migrations.AlterField(
            model_name='person',
            name='salary',
            field=models.FloatField(blank=True, null=True),
        ),
    ]

# Generated by Django 3.2 on 2022-05-27 14:13

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='filmwork',
            name='creation_date',
            field=models.DateField(blank=True, null=True, verbose_name='creation_date'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='description',
            field=models.TextField(blank=True, null=True, verbose_name='description'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='rating',
            field=models.FloatField(blank=True, default=0, null=True, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(100)], verbose_name='rating'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='type',
            field=models.TextField(choices=[('movie', 'Movie'), ('tw_show', 'Tw Show')], verbose_name='type'),
        ),
        migrations.AlterField(
            model_name='genre',
            name='description',
            field=models.TextField(blank=True, null=True, verbose_name='description'),
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='role',
            field=models.TextField(choices=[('actor', 'Actor'), ('director', 'Director'), ('writer', 'Writer')], null=True, verbose_name='role'),
        ),
    ]
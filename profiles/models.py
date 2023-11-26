from django.db import models

class Person(models.Model):
    id = models.IntegerField(primary_key=True)
    first_name = models.CharField(max_length=25)
    last_name = models.CharField(max_length=25)
    email = models.EmailField(max_length=40, null=True, blank=True)
    gender = models.CharField(max_length=1, null=True, blank=True)
    date_of_birth = models.DateField(null=True, blank=True)
    industry = models.CharField(max_length=65, null=True, blank=True)
    salary = models.IntegerField(null=True, blank=True)
    years_of_experience = models.IntegerField(null=True, blank=True)
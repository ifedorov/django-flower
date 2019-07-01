from django.db import models


class CeleryWorker(models.Model):
    id = models.CharField("ID", max_length=512,
                          primary_key=True,
                          db_index=True)
    name = models.CharField("Name", max_length=512)
    active = models.BooleanField("active", default=False)


class CeleryEvent(models.Model):
    worker = models.ForeignKey(CeleryWorker, verbose_name="Worker")
    event = models.CharField("Event", max_length=32)
    counter = models.BigIntegerField("Counter", default=0)


class CeleryTask(models.Model):
    uuid = models.UUIDField("UUID", primary_key=True)
    name = models.CharField("Name", max_length=512)
    state = models.CharField("State", max_length=32)
    worker = models.ForeignKey(CeleryWorker, verbose_name="Worker")
from django.db import models


class CeleryWorker(models.Model):
    id = models.CharField("ID", max_length=512,
                          primary_key=True,
                          db_index=True)
    name = models.CharField("Name", max_length=512)
    alive = models.BooleanField("Alive", default=False)


class CeleryTask(models.Model):
    uuid = models.UUIDField("UUID", primary_key=True)
    name = models.CharField("Name", max_length=512)
    state = models.CharField("State", max_length=32)
    worker = models.CharField("Worker", max_length=512)

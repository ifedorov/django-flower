from django.db import models


class CeleryWorker(models.Model):
    name = models.CharField("Name", max_length=512,
                            primary_key=True,
                            db_index=True)
    active = models.BooleanField("active", default=False)
    status = models.CharField("Status", max_length=32)


class CeleryEvent(models.Model):
    worker = models.ForeignKey(CeleryWorker, verbose_name="Worker")
    event = models.CharField("Event", max_length=32)
    counter = models.BigIntegerField("Counter", default=0)


class CeleryTask(models.Model):
    uuid = models.UUIDField("UUID", primary_key=True)
    name = models.CharField("Name", max_length=512)
    state = models.CharField("State", max_length=32)
    worker = models.ForeignKey(CeleryWorker, verbose_name="Worker")

    def as_dict(self):
        dk = {}
        for field in self._meta.get_fields():
            dk[field.name] = getattr(self, field.name)
        return dk


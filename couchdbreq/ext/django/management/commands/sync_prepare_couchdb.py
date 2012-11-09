from django.db.models import get_apps
from django.core.management.base import BaseCommand
from couchdbreq.ext.django.loading import couchdbkit_handler

class Command(BaseCommand):
    help = 'Sync design docs to temporary ids'

    def handle(self, *args, **options):
        for app in get_apps():
            couchdbkit_handler.sync(app, verbosity=2, temp='tmp')
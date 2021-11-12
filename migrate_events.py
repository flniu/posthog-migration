import sys

import posthog
from peewee import (
    Model,
    IntegerField,
    CharField,
    DateTimeField,
    BooleanField,
    UUIDField,
)

from playhouse.postgres_ext import PostgresqlExtDatabase, BinaryJSONField


pg_db = PostgresqlExtDatabase(
    database="posthog",
    user="postgres",
    password="postgres",
    host="localhost",
    port=15432,
)


class PosthogEvent(Model):
    class Meta:
        database = pg_db
        table_name = "posthog_event"

    id = IntegerField(primary_key=True)
    event = CharField(null=True)
    properties = BinaryJSONField(null=False)
    elements = BinaryJSONField(null=True)
    timestamp = DateTimeField(null=False)
    team_id = IntegerField(null=False)
    distinct_id = CharField(null=False)
    elements_hash = CharField(null=True)
    created_at = DateTimeField(null=True)
    site_url = CharField(null=True)


class PosthogPerson(Model):
    class Meta:
        database = pg_db
        table_name = "posthog_person"

    id = IntegerField(primary_key=True)
    created_at = DateTimeField(null=False)
    properties = BinaryJSONField(null=False)
    team_id = IntegerField(null=False)
    is_user_id = IntegerField(null=True)
    is_identified = BooleanField(null=False)
    uuid = UUIDField(null=False)


class PosthogPersonDistinctID(Model):
    class Meta:
        database = pg_db
        table_name = "posthog_persondistinctid"

    id = IntegerField(primary_key=True)
    distinct_id = CharField(null=False)
    person_id = IntegerField(null=False)
    team_id = IntegerField(null=False)


def setup_posthog():
    posthog.project_api_key = "phc_NrktLVE9OHS7l5Iyg4mwiSbFL65UUm3JqMiN2XGNrWH"
    posthog.host = "https://local.me"
    posthog.debug = True


def migrate_events():
    with pg_db:
        events = PosthogEvent.select().order_by(PosthogEvent.id).namedtuples()
        for event in events:
            print(
                f"Migrate event: id={event.id} event={event.event} timestamp={event.timestamp.isoformat()}"
            )
            try:
                posthog.capture(
                    event.distinct_id,
                    event.event,
                    properties=event.properties,
                    timestamp=event.timestamp,
                )
            except Exception as e:
                print(e, file=sys.stderr)


def migrate_persons():
    with pg_db:
        persons = (
            PosthogPerson.select(
                PosthogPerson.id,
                PosthogPerson.created_at,
                PosthogPerson.properties,
                PosthogPersonDistinctID.distinct_id,
            )
            .join(
                PosthogPersonDistinctID,
                on=(PosthogPerson.id == PosthogPersonDistinctID.person_id),
            )
            .order_by(PosthogPersonDistinctID.id)
            .namedtuples()
        )
        for person in persons:
            print(
                f"Migrate person: id={person.id} distinct_id={person.distinct_id} created_at={person.created_at.isoformat()}"
            )
            try:
                posthog.identify(
                    person.distinct_id,
                    properties=person.properties,
                    timestamp=person.created_at,
                )
            except Exception as e:
                print(e, file=sys.stderr)


if __name__ == "__main__":
    setup_posthog()

    migrate_events()
    migrate_persons()

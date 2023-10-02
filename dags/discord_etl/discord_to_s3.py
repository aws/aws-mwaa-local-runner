from shared import s3
from shared.irondata import Irondata
from discord_etl.discord_client import DiscordDataClient


def discord_extract(ds, bucket, guild_id, guild_name, members_table, **kwargs):
    client = DiscordDataClient(guild_id=guild_id)
    client.run(Irondata.get_config("DISCORD_AUTH_TOKEN"))

    members = _fetch_members(client)
    if members:
        member_csv = _process_members(members)
        member_count = len(member_csv)
        s3.upload_as_csv(
            bucket,
            f"{members_table.schema_in_env}/{guild_name}/{ds}/{members_table.table_in_env}",
            member_csv)

        print(f"{member_count} members found. Written to S3.")
        return True
    else:
        print("Found no members via Discord API")
        return False


def _fetch_members(client):
    return client.get_members()


def _process_members(members):
    rows = [_build_row_from_member(member) for member in members]
    return rows


def _build_row_from_member(member):
    return [
        member.id,
        member.name,
        member.display_name,
        member.nick,
        [role.name for role in member.roles],
        member.guild.id,
        member.guild.name,
        member.created_at,
        member.joined_at,
        False, # is_removed
    ]

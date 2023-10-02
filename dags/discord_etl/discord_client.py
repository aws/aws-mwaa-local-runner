import discord


class DiscordDataClient(discord.Client):
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True

    def __init__(self, guild_id: int, intents=intents, loop=None, **options):
        discord.Client.__init__(self, intents=intents, loop=loop, **options)
        self.members = []
        self.guild_id = guild_id

    async def on_ready(self):
        guild = discord.utils.get(self.guilds, id=self.guild_id)
        self.members = guild.members
        await self.close()

    def get_members(self):
        return self.members

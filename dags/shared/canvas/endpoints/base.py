from ..base import Canvas

class CanvasEndpoint(Canvas):

    endpoint:str

    @classmethod
    def construct_request(self, endpoint):
        return f'{self.root}{self.endpoint}{endpoint}'

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} id={self.id}>'



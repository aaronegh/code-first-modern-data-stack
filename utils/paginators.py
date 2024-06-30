from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request


class QueryParamPaginator(BasePaginator):
    def __init__(self, 
                 page_param: str, 
                 initial_page: int
                ):
                super().__init__()
                self.page_param = page_param
                self.page = initial_page

    def update_state(self, 
                     response: Response
                     ) -> None:
        # Assuming the API returns an empty list when no more data is available
        if not response.json():
            self._has_next_page = False
        else:
            self.page += 1

    def update_request(self, 
                       request: Request
                       ) -> None:
        if request.params is None:
            request.params = {}
        request.params[self.page_param] = self.page
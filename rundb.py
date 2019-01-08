import requests


class DB:
    """Wrapper around the RunDB API"""
    prefix = "http://xenon-runsdb-dev.grid.uchicago.edu:5000"

    headers = {'Content-Type': "application/json",
               'Authorization': "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1Mzg0MT"
                                "g1NjMsImV4cCI6MTUzODUwNDk2MywicmZfZXhwIjoxNTQxMDEwNTYzLCJqdGki"
                                "OiI0N2I0MzZjMy1mZGNlLTQxY2YtODM1NS0zNTkyYTZhNjhjMDIiLCJpZCI6Mi"
                                "wicmxzIjoiYWRtaW4scHJvZHVjdGlvbix1c2VyIn0.19tnPXs8AnUsVurDo7QT"
                                "FDALN5D9k3WQn8tadVIvPzc",
               'Cache-Control': "no-cache",
               'Postman-Token': "52764fd0-b9d2-4a67-8d9d-09056ee8732f"
    }

    def get(self, path):
        req = self.prefix + path
        print(req)
        return requests.get(req, headers=self.headers)


    def get_name(self, number):
        path = f"/runs/number/{number}/filter/name"
        return self.get(path)


if __name__ == "__main__":
    db = DB()
    x = db.get_name(10000)
    print(x.text)
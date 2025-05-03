import asyncio
import aiohttp  # pip install aiohttp aiodns
from itertools import chain
import pandas as pd


async def fetch_wikidata(session: aiohttp.ClientSession, id_=None, query=None) -> dict:
    url = "https://www.wikidata.org/w/api.php/"
    data = {}
    if id_:
        params = {
            "action": "wbgetentities",
            "ids": id_,
            "format": "json",
            "languages": "en",
        }

    if query:
        params: dict = {
            "action": "wbsearchentities",
            "format": "json",
            "search": query,
            "language": "en",
        }

    resp = await session.request(method="get", url=url, params=params)
    # Note that this may raise an exception for non-2xx responses
    # You can either handle that here, or pass the exception through
    data = await resp.json()
    return data


async def property_value(property_id: str, claims: dict) -> list:
    values = []
    ids = []
    # print(claims)
    async with aiohttp.ClientSession() as session:
        for i, v in enumerate(claims[property_id]):
            if prop_value := v["mainsnak"].get("datavalue"):
                if isinstance(value := prop_value.get("value"), str):
                    values.append(value)
                elif wiki_id := value.get("id"):
                    ids.append(wiki_id)
                else:
                    values.append(value.get("time", value.get("text")))

        id_tasks = [fetch_wikidata(session=session, id_=id_) for id_ in ids]
        ids = await asyncio.gather(*id_tasks, return_exceptions=True)
        # ids = chain(ids)
        ids = [
            labels.get("value")
            for d in ids
            for prop_id in d["entities"].values()
            for labels in prop_id.get("labels").values()
        ]

        values.extend(ids)

        # print(f'{values=}')

    return values


async def main(query, threshold=5):
    # Asynchronous context manager.  Prefer this rather
    # than using a different session for each GET request

    valid_properties = {
        "P31": "type of entity",  # instance of
        "P571": "inception",
        "P112": "founded_by",
        "P17": "country",
        "P856": "official_website",
        "P452": "industry",
        "P1448": "official_name",
        "P169": "CEO",
        "P1451": "motto",
        "P749": "parent organisation",
        "P1056": "products/materials produced",
        "P127": "Owned by",
        "P279": "Subclass of",
        "P178": "developer",
        "P275": "Copyright License",
        "P1830": "Owner of",
        "P355": "has subsidiary",
        "P577": "Publication date",
        "P1716": "Brand",
        "P155": "Follows",
        "P156": "Followed by",
    }
    async with aiohttp.ClientSession() as session:
        search_tasks = [fetch_wikidata(session=session, query=query)]
        # asyncio.gather() will wait on the entire task set to be
        # completed.  If you want to process results greedily as they come in,
        # loop over asyncio.as_completed()
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)
    data = {
        d["id"]: {"label": d["label"], "description": d.get("description", "")}
        for elem in search_results
        for d in elem["search"]
    }
    claim_entities: list[str] = [
        d["id"] for elem in search_results for d in elem["search"]
    ]

    async with aiohttp.ClientSession() as session:
        id_tasks = [
            fetch_wikidata(
                session=session,
                id_=claim_entity,
            )
            for claim_entity in claim_entities
        ]
        claim_results = await asyncio.gather(*id_tasks, return_exceptions=True)

    for claim_result in claim_results:
        wikidata_id = list(claim_result["entities"].keys())[0]
        wikidata_id_content = claim_result["entities"][wikidata_id]
        if (
            len(
                properties := set(valid_properties.keys())
                & set(wikidata_id_content["claims"].keys())
            )
            < threshold
        ):
            del data[wikidata_id]
            continue
        data[wikidata_id]["aliases"]: list = list(
            set(
                alias["value"]
                for alias in chain.from_iterable(
                    wikidata_id_content["aliases"].values()
                )
            )
        )

        for p in properties:
            property_tasks = []
            property_tasks.append(property_value(p, wikidata_id_content["claims"]))
            data[wikidata_id][valid_properties[p]] = await asyncio.gather(
                *property_tasks, return_exceptions=True
            )

            data[wikidata_id][valid_properties[p]] = list(
                chain(*data[wikidata_id][valid_properties[p]])
            )
    return data


def data_to_df(data: dict):
    df = pd.DataFrame.from_dict(data, orient="index")
    # df = df.explode('inception')
    # df['inception'] = df['inception'].str.replace(pat=r'[-+]([0-9]{4}-[0-9]{2}-[0-9]{2}).+',repl= r'\1', regex=True)

    return df


if __name__ == "__main__":
    query = input("query: ")
    data = asyncio.run(main(query))
    # print(f'{data=}')
    df = data_to_df(data).T
    print(df)

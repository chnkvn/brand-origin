import asyncio
from itertools import chain

import aiohttp  # pip install aiohttp aiodns
import pandas as pd
import streamlit as st


async def fetch_wikidata(session: aiohttp.ClientSession, id_=None, query=None) -> dict:
    # print(f"{id_=}", f"{query=}")
    url = "https://www.wikidata.org/w/api.php/"
    data = {}
    params = dict()
    if id_:  # 'entity / property'
        params = {
            "action": "wbgetentities",
            "ids": "|".join(id_),
            "format": "json",
            "languages": "en",
        }

    if query:  # 'natural language query'
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

    return data["entities"] if id_ else data["search"]


async def property_value(property_id: str, claims: dict) -> list:
    values = []
    ids = []
    # Parse data
    for i, v in enumerate(claims[property_id]):
        if prop_value := v["mainsnak"].get("datavalue"):
            if isinstance(value := prop_value.get("value"), str):
                values.append(value)
            elif wiki_id := value.get("id"):
                ids.append(wiki_id)
            else:
                values.append(value.get("time", value.get("text")))
    # Get labels of entities if needed
    if len(ids) > 0:
        async with aiohttp.ClientSession() as session:
            id_tasks = [fetch_wikidata(session=session, id_=ids)]
            ids = await asyncio.gather(*id_tasks, return_exceptions=True)
            # ids = chain(ids)

            ids = [
                labels.get("value")
                # prop_id.keys()
                for d in ids
                for prop_id in d.values()
                for labels in prop_id.get("labels").values()
            ]

            values.extend(ids)

    return property_id, values


async def main(query, threshold=4):
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

    # Natural language query
    async with aiohttp.ClientSession() as session:
        search_tasks = [fetch_wikidata(session=session, query=query)]
        # asyncio.gather() will wait on the entire task set to be
        # completed.  If you want to process results greedily as they come in,
        # loop over asyncio.as_completed()
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

    # get label, description
    data = {
        d["id"]: {"label": d["label"], "description": d.get("description", "")}
        for d in chain(*search_results)
    }

    # claim entities
    claim_entities: list[str] = [d["id"] for d in chain(*search_results)]
    if len(claim_entities) < 1:
        raise NameError(f"No results found for {query}")
    async with aiohttp.ClientSession() as session:
        id_tasks = [
            fetch_wikidata(
                session=session,
                id_=claim_entities,
            )
        ]
        claim_results = await asyncio.gather(*id_tasks, return_exceptions=False)

    # parse data
    for entity, v in claim_results[0].items():
        if (
            len(properties := set(valid_properties.keys()) & set(v["claims"].keys()))
            < threshold
        ):
            del data[entity]
            continue

        data[entity]["aliases"]: list = list(
            set(alias["value"] for alias in chain.from_iterable(v["aliases"].values()))
        )
        property_tasks = [property_value(p, v["claims"]) for p in properties]

        req = await asyncio.gather(*property_tasks, return_exceptions=True)

        for prop, value in req:
            data[entity][valid_properties[prop]] = value
    return data


def data_to_df(data: dict):
    df = pd.DataFrame.from_dict(data, orient="index")
    # df = df.explode('inception')
    # df['inception'] = df['inception'].str.replace(pat=r'[-+]([0-9]{4}-[0-9]{2}-[0-9]{2}).+',repl= r'\1', regex=True)

    return df


async def app():
    """ """
    st.set_page_config(
        page_title="Brand origin",
        page_icon=None,
        layout="wide",
        initial_sidebar_state="auto",
        menu_items=None,
    )

    st.title("Brand origin")
    brand = st.text_input(
        "Please input the name of the brand to retrieve", value="streamlit"
    )
    # lang =
    if brand:
        try:
            data = await main(brand)
            df = data_to_df(data)
            st.dataframe(df, use_container_width=True)
        except NameError as e:
            st.error(e)
            st.error("Please try with another query.")


if __name__ == "__main__":
    asyncio.run(app())

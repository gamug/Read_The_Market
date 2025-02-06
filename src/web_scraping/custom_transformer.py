from typing import List, Tuple, Union
from langchain_community.document_transformers.beautiful_soup_transformer import get_navigable_strings
from langchain.document_transformers import BeautifulSoupTransformer


class CustomSoupTransformer(BeautifulSoupTransformer):
    @staticmethod
    def extract_tags(
        html_content: str,
        tags: Union[List[str], Tuple[str, ...]],
        *,
        remove_comments: bool = False,
        sep: str = '|'
    ) -> str:
        """
        Extract specific tags from a given HTML content.

        Args:
            html_content: The original HTML content string.
            tags: A list of tags to be extracted from the HTML.

        Returns:
            A string combining the content of the extracted tags.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html_content, "html.parser")
        text_parts: List[str] = []
        for element in soup.find_all():
            if element.name in tags:
                # Extract all navigable strings recursively from this element.
                text_parts += get_navigable_strings(
                    element, remove_comments=remove_comments
                )

                # To avoid duplicate text, remove all descendants from the soup.
                element.decompose()
        text_parts = [element for element in text_parts if element!='']
        return sep.join(text_parts)
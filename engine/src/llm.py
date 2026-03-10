import os
import sys
import json
from openai import OpenAI
from google import genai
from google.genai import types as gtypes
import anthropic


import keyword_synonyms

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from shared_python.src import loadenv

DEFAULT_SYSTEM_PROMPT = "You are a friendly and helpful assistant."

KEYWORD_EXTRACTION_SYSTEM_PROMPT = """
You are part of the Bookrover book recommendation engine, used to search a vast database of fiction books and their associated metadata to provide reading list recommendations based on user queries. Your job is to analyze the user query and identify keywords the engine can use as part of a keyword search. This keyword search will be used alongside other data, including embedding-based semantic search, to create reading lists for the user. Think through the problem before providing your answer.

Keywords are one- or two-word strings, all-lowercase, and may contain punctuation. All keywords must either be present within the prompt, as a synonym, antonym, tense, or derivative, or necessarily implied by the prompt. Supplement keywords with common synonyms wherever possible. For example, do not add a genre as a tag just because the query asks for an author known for writing in a particular genre, but do add that genre if the query requests an element that is unheard of outside of that genre, such as wizards or aliens. Correct any obvious spelling mistakes.

Tag each keyword with at least one of six categories: authors, title, genre, mood, content warnings, and description. The keyword search will look for the keyword in its associated field in the book database. For example, "King" in the authors field will match a book by Stephen King, but not a book titled "King of Wrath". You can repeat the same keyword in multiple categories if appropriate. Do not assume every category will have a keyword.

Also tag each keyword with one of four strengths: mandatory, positive, negative, and disqualifying.

- Mandatory keywords are terms that must be present in a book's metadata to satisfy the query. Books without all mandatory keywords will be removed from the pool of books considered for retrieval. For example, "Show me books by Andy Weir" has "Andy Weir" as a mandatory authors keyword.
- Positive keywords are terms that suggest that a book is more likely to satisfy the user's query if they are present in the book's metadata. These will be used to help determine the book's final ranking in the system's response. For example, "Show me books about dragons" has "fantasy" as a positive genre keyword.
- Negative keywords are terms that suggest that a book is less likely to satisfy the user's query if they are present in the book's metadata. These will be used to help determine the book's final ranking in the system's response. For example, "Show me fast-paced books" has "slow-paced" as a negative mood keyword.
- Disqualifying keywords are terms that must be absent in a book's metadata to satisfy the query. Books with any disqualifying keywords will be removed from the pool of books considered for retrieval. For example, "Show me books without sexual content" has "sexual content" as a disqualifying content warning keyword.

Use mandatory and disqualifying keywords sparingly, and only if the user directly states that the content must be present or absent. Tags with these strengths combine with each other and across categories as AND conditions, not OR, so poorly-chosen keywords might eliminate all available books in the database, which we don't want. When including synonyms, only the most common synonym may be tagged as mandatory or disqualifying: the others must be positive or negative. Be mindful that book tags are user-defined, so similar content may be described in multiple ways in book metadata. Tags in the description category should never be mandatory or disqualifying.

Also tag each keyword with a justification, in 100 words or less, for why you chose that keyword, its weight, and its category. This justification will be used as an aid for debugging.

Choose keywords, strength, and categories to maximize the chance that the user finds what they are looking for. Do not fill in information based on your trained knowledge of any books, authors, or genre trends mentioned; any inferences must come from the prompt itself.

Provide your response in the following JSON format:

{
    "keywords": [
        {
            "keyword": "keyword1",
            "strength": "strength1",
            "categories": ["category1_1", "category1_2"],
            "justification": "justification1"
        },
        {
            "keyword": "keyword2",
            "strength": "strength2",
            "categories": ["category2_1", "category2_2"],
            "justification": "justification2"
        },
        {
            "keyword": "keyword3",
            "strength": "strength3",
            "categories": ["category3_1", "category3_2"],
            "justification": "justification3"
        },
    ]
}

Provide only the JSON response. Do not append or prepend any additional text to your response. Rely only on the prompt given, and not your trained knowledge of any books, authors, or genres mentioned.

For example:

Example query 1:
"Show me young adult stories with queerr themes. I don't want anything smutty or by J.K. Rowling."

Example response 1:
{
    "keywords": [
        {
            "keyword": "J.K. Rowling",
            "strength": "disqualifying",
            "categories": ["authors"],
            "justification": "The user directly stated that they do not want to read books by this author. This is very specific and explicit, so we should treat this as a hard condition."
        },
        {
            "keyword": "young adult",
            "strength": "positive",
            "categories": ["genre"],
            "justification": "The user directly stated that they want to read books in the young adult genre, but they might not be averse to new adult or middle-grade books. We already have a hard condition, so we don't want to be too restrictive."
        },
        {
            "keyword": "queer",
            "strength": "positive",
            "categories": ["genre", "description"],
            "justification": "Corrected spelling of 'queer'. The user stated directly that they prefer queer themes. This may appear in the genre or description."
        },
        {
            "keyword": "lbgtq",
            "strength": "positive",
            "categories": ["genre", "description"],
            "justification": "More common synonym for 'queer', and more likely to appear in the book's metadata."
        },
        {
            "keyword": "lgbt",
            "strength": "positive",
            "categories": ["genre", "description"],
            "justification": "More common synonym for 'queer'."
        },
        {
            "keyword": "smutty",
            "strength": "negative",
            "categories": ["mood"],
            "justification": "The user stated that they don't want to read smutty books. In the adjective form, this is most likely to appear in mood tags."
        },
        {
            "keyword": "smut",
            "strength": "negative",
            "categories": ["content warnings"],
            "justification": "Noun form of 'smutty'. This form is more likely to appear as a content warning."
        },
        {
            "keyword": "sex",
            "strength": "negative",
            "categories": ["content warnings"],
            "justification", "More common synonym for 'smut'. This is most likely to appear as a content warning. It may also appear in the description, but matching there may falsely exclude mentions of sexuality or gender identity, which are common queer themes, so we exclude it from that category."
        },
        {
            "keyword": "erotic",
            "strength": "negative",
            "categories": ["mood", "content warnings"],
            "justification": "More common synonym for 'smutty'. This form may appear on its own as a mood or as part of 'erotic content' as a content warning."
        }
    ]
}

Example query 2:
"I loved Project Hail Mary, and I want to read something similar."

Example response 2:
{
    "keywords": [
        {
            "keyword": "project hail",
            "strength": "disqualifying",
            "categories": ["title"],
            "justification": "The user has already read Project Hail Mary, so surely does not want it recommended as their next read. 'project hail' is the two-word substring that excludes the fewest other books. The prompt only includes the title, so I must not assume or recall any other information about this book."
        }
    ]
}

Note on example 2: The user did not ask for any specific book elements. Prompts like these are best served by semantic similarity search, not keyword search. Do not rely on your knowledge of the book to fill in the blanks.
"""

KEYWORD_STRENGTHS = ["mandatory", "positive", "negative", "disqualifying"]
STRONG_KEYWORD_STRENGTHS = ["mandatory", "disqualifying"]
WEAK_KEYWORD_STRENGTHS = ["positive", "negative"]
POSITIVE_KEYWORD_STRENGTHS = ["mandatory", "positive"]
NEGATIVE_KEYWORD_STRENGTHS = ["negative", "disqualifying"]

KEYWORD_CATEGORIES = ["authors", "title", "genre", "mood", "content warnings",
                      "description"]

def LLMRequest(user_prompt:str, system_prompt:str = DEFAULT_SYSTEM_PROMPT) -> str:
    '''
    LLMRequest: Sends a request to the LLM. Calls the appropriate helper function based on
                LLM_TYPE .env variable.
    
    :param user_prompt: The user's query.
    :type tag: str
    :param system_prompt: The system prompt, with detailed instructions on how to process the
                          user's query.
    :type category: str
    :return: The LLM's response.
    :rtype: str
    '''
    llm_type = loadenv.loadEnvVariable("LLM_TYPE")

    match llm_type:
        case "OpenAI" | "ChatGPT":
            return OpenAIRequest(user_prompt, system_prompt)
        case "Google" | "Gemini":
            return GeminiRequest(user_prompt, system_prompt)
        case "Anthropic" | "Claude":
            return ClaudeRequest(user_prompt, system_prompt)
        case _:
            raise NotImplementedError(f"LLM provider {llm_type} not supported.")

def OpenAIRequest(user_prompt:str, system_prompt:str) -> str:
    '''
    OpenAIRequest: Sends a request to an OpenAI (ChatGPT) LLM.
    
    :param user_prompt: The user's query.
    :type tag: str
    :param system_prompt: The system prompt, with detailed instructions on how to process the
                          user's query.
    :type category: str
    :return: The LLM's response.
    :rtype: str
    '''
    client = OpenAI(api_key=loadenv.loadEnvVariable("LLM_API_KEY"))

    response = client.responses.create(
        model = loadenv.loadEnvVariable("LLM_MODEL"),
        instructions = system_prompt,
        input = user_prompt
    )

    return response.output_text

def GeminiRequest(user_prompt:str, system_prompt:str) -> str:
    '''
    OpenAIRequest: Sends a request to a Google (Gemini) LLM.
    
    :param user_prompt: The user's query.
    :type tag: str
    :param system_prompt: The system prompt, with detailed instructions on how to process the
                          user's query.
    :type category: str
    :return: The LLM's response.
    :rtype: str
    '''
    client = genai.Client(api_key=loadenv.loadEnvVariable("LLM_API_KEY"))

    response = client.models.generate_content(
        model = loadenv.loadEnvVariable("LLM_MODEL"),
        contents = f"{system_prompt}\n\nUser prompt: {user_prompt}"
    )

    if response.text:
        return response.text
    else:
        return "Error: No reponse from LLM."


def ClaudeRequest(user_prompt:str, system_prompt:str) -> str:
    '''
    OpenAIRequest: Sends a request to an Anthropic (Claude) LLM.
    
    :param user_prompt: The user's query.
    :type tag: str
    :param system_prompt: The system prompt, with detailed instructions on how to process the
                          user's query.
    :type category: str
    :return: The LLM's response.
    :rtype: str
    '''
    client = anthropic.Anthropic(api_key=loadenv.loadEnvVariable("LLM_API_KEY"))

    response = client.messages.create(
        model = loadenv.loadEnvVariable("LLM_MODEL"),
        max_tokens=1024,
        messages = [
            {"role": "user", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
    )

    content = response.content[-1]
    if content.type == "text":
        return content.text
    else:
        return "Error: No reponse from LLM."


def extractKeywords(user_prompt: str) -> list[dict]:
    '''
    extractKeywords: Uses the LLM to extract relevant keywords from a user's query. Read the
                     KEYWORD_EXTRACTION_SYSTEM_PROMPT variable for the exact instructions
                     passed, including details of the return format.

    :param user_prompt: The user's query.
    :type tag: str
    :rtype: list[dict]
    :return: A list of dictionaries with the following structure:
    - "keyword" (str): An identified keyword.
    - "strength" (str): The strength of the keyword, from "mandatory", "positive",
                        "negative", or "disqualifying".
    - "categories" (list[str]): A list of the fields in the book metadata where the keyword
                                will be searched for. 
    - "justification" (str): The LLM's justification for the keyword's entry, used mainly
                             for debugging.
    '''
    default_output: list[dict] = []

    max_attempts = 5
    for attempt in range(max_attempts):
        if attempt == 0:
            print('Parsing LLM response.')
        elif attempt == 4:
            print(f'Requerying LLM: Last attempt.')
        else:
            print(f'Requerying LLM: {max_attempts - attempt} attempts remaining.')

        result = LLMRequest(user_prompt, KEYWORD_EXTRACTION_SYSTEM_PROMPT)
        # print(result)
    
        # Strip any additional characters before or after the curly braces.
        json_start_index = result.find('{')
        json_end_index = result.rfind('}')
        if ((json_start_index == -1) or (json_end_index == -1)
            or (json_start_index > json_end_index)):
            print(f"Response not in JSON format.")
            continue
            
        # Parse JSON response into a Python object, and thoroughly test that the
        # format is correct.
        try:
            json_response = json.loads(result[json_start_index: json_end_index + 1])
        except json.JSONDecodeError:
            print(f"Response could not be parsed as JSON.")
            continue

        if (not json_response) or (not isinstance(json_response, dict)):
            print(f"JSON response is not a dictionary.")
            continue
        
        if (("keywords" not in json_response)
            or (not isinstance(json_response["keywords"], list))):
            print(f"List of keywords not present in JSON response.")
            continue

        if not json_response["keywords"]:
            # An empty list of keywords is a valid, and sometimes correct, response.        
            return default_output
        
        bad_item_found = False
        canonical_tags: list[dict] = []
        for item in json_response["keywords"]:
            if (not isinstance(item, dict)
                or ("keyword" not in item) or (not isinstance(item["keyword"], str))
                or ("strength" not in item) or (not isinstance(item["strength"], str))
                or (item["strength"] not in KEYWORD_STRENGTHS)
                or ("categories" not in item) or (not isinstance(item["categories"], list))
                or ("justification" not in item) or (not isinstance(item["justification"], str))
            ):
                print("JSON response improperly structured.")
                bad_item_found = True
                break
            
            if item["strength"] not in KEYWORD_STRENGTHS:
                print(f'Keyword "{item["keyword"]}" has bad strength value "{item["strength"]}".')
                bad_item_found = True
                break

            for category in item["categories"]:
                if category not in KEYWORD_CATEGORIES:
                    print(f'Keyword "{item["keyword"]}" has bad category value "{category}".')
                    bad_item_found = True
                    break
            if bad_item_found:
                break

            # Add canonical synonyms, and weaken the non-canonical form when found.
            for category in item["categories"]:
                canonical_tag = keyword_synonyms.getCanonicalTag(item["keyword"], category)
                if canonical_tag:
                    canonical_tag_item = {}
                    canonical_tag_already_in_response = False
                    for item2 in json_response["keywords"]:
                        if (canonical_tag == item2["keyword"]) and (category in item2["categories"]):
                            canonical_tag_item = item2
                            canonical_tag_already_in_response = True
                            break
                    if not canonical_tag_already_in_response:
                        canonical_tags.append({
                            "keyword": canonical_tag,
                            "strength": item["strength"],
                            "categories": [category],
                            "justification": f"Canonical tag for {item["keyword"]}"
                        })
                        canonical_tag_item = canonical_tags[-1]

                    if item["strength"] == "mandatory":
                        item["strength"] = "positive"
                    elif item["strength"] == "disqualifying":
                        canonical_tag_item["strength"] = "negative"

        if bad_item_found:
            continue

        # We've survived the gauntlet, so our response is good. Add in the canonical
        # keywords and return the response.
        json_response["keywords"].extend(canonical_tags)
        return json_response["keywords"]
    
    # We've ran out of attempts
    print("No more attempts. Ensure the LLM is working correctly, or switch to a different service, and try again.")
    print("Returning empty keyword dictionary.")
    return default_output

def main() -> None:
    '''
    main: A simple test that the LLM is working correctly.
    
    :return: None
    :rtype: None
    '''
    # Test that the LLM can receive and respond to prompts.
    print(extractKeywords("Not fantasy. Unless it’s low fantasy. Actually, maybe sci-fi. But no space."))

if __name__ == "__main__":
    main()
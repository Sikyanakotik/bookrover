GENRE_SYNONYMS = { # Maps from Hardcover's canonical forms.
                   # Includes subgenres that don't already include the main genre's name.
    "fantasy": ["sword and sorcery", "sword & sorcery", "secondary world",
                "isekai", "litrpg", "lit rpg", "magic", "fantasy fiction"],
    "young adult": ["ya", "teen", "teens", "new adult", "youngadult"],
    "science fiction": ["science", "space", "sci fi", "scifi", "sci-fi", "syfy",
                        "speculative fiction", "spec fic", "genre fiction",
                        "sciencefiction", "science fantasy", "space opera",
                        "dystopian", "utopian", "post-apocalyptic"],
    "romance": ["romantasy", "erotica", "romantic drama", "romantic", "romantic comedy",
                "romcom", "rom com", "romantic tragedy", "love story", "love stories"],
    "thriller": ["suspense", "psychological", "page-turner", "pageturner", "action",
                 "airport fiction"],
    "mystery": ["whodunnit", "murder", "detective", "noir", "crime", "true crime",
                "truecrime", "hardboiled", "film noir", "filmnoir", "legal thriller",
                "police procedural", "police"],
    "horror": ["paranormal", "gothic", "ghost story", "survival", "zombie", "vampire"],
    "juvenile fiction": ["kids", "kid lit", "kiddy lit", "children's", "childrens",
                         "young readers", "early readers", "boys", "girls", "boys'",
                         "girls'", "kids'", "childrens stories", "children's stories"],
    "literary": ["literature", "serious", "serious fiction", "high literature",
                  "artistic literature", "lit", "lit fic", "magical realism", 
                  "prose", "narrative poetry", "tragedy", "slice of life"],
    "classics": ["epic", "saga", "epic poetry", "epic poem" "classical", 
                 "classical literature", "folklore"],
    "humor": ["humour", "satire", "parody", "comedy", "comic fantasy", "dark comedy",
              "absurdist", "absurdism", "surrealist", "surrealism"],
    "adventure": ["swashbuckler", "swashbuckling", "quest", "travellogue", "picaresque",
                  "pulp"],
    "war": ["military", "military fiction", "combat", "tactics"],
    "lgbtq": ["queer", "queer media", "lbgt", "lgbt+", "lgbtq+", "2slgbtqi+",
              "queer romance"],
    "gay": ["mlm", "boys love", "yaoi"],
    "lesbian": ["wlw", "girls love", "yuri"],
    "transgender": ["tg", "ftm", "female-to-male", "mtf", "male-to-female", "trans",
                    "trans literature", "gender identity"],
    "nonbinary": ["non-binary", "enby", "demiboy", "demigirl", "genderqueer",
                   "third gender"],
    "friendship": ["found family"],
    "western": ["cowboy", "wild west", "old west", "gunsligner", "frontier",
                "American frontier"]
}

MOOD_SYNONYMS = {
    "adventurous": ["adventure", "swashbuckling", "picaresque", "pulpy", "two-fisted"],
    "emotional": ["dramatic", "moving", "challenging", "haunting"],
    "dark": ["grimdark", "oppressive", "gritty", "pessimistic", "gloomy"],
    "mysterious": ["compelling", "intriguing", "enigmatic", "fascinating", "puzzling",
                   "magical", "fantastic", "arcane"],
    "tense": ["unsettling", "creepy", "nail-biting", "stressful", "suspenseful",
               "suspense"],
    "reflective": ["pensive", "deep", "insightful", "navel-gazing", "meditative"],
    "funny": ["humorous", "humourous", "comedic", "laugh-out-loud", "satirical",
               "hilarious"],
    "lighthearted": ["breezy", "fluffy", "feel-good", "unserious", "carefree", "fun"],
    "sad": ["depressing", "melancholy", "tearjerker", "tragic", "mourning"],
    "hopeful": ["inspiring", "defiant", "optimistic", "heartening", "upbeat", "resolute"],
    "informative": ["educational", "pedantic", "educative", "informational",
                    "instructive", "practical", "useful", "helpful", "detailed"],
    "relaxing": ["cozy", "calming", "warm", "comfortable", "soothing", "comforting"],
    "fast-paced": ["brisk", "page-turning", "page-turner", "fast", "rapid", "breakneck",
                   "frenetic", "dynamic", "rushed", "hurried", "helter-skelter"],
    "slow-paced": ["slow", "lazy", "deliberate", "methodical", "leisurely", "gradual",
                   "plodding", "sluggish", "glacial", "ponderous", "slow burn"],
    "exciting": ["invigorating", "invigourating", "action-packed", "high-action",
                 "thrilling", "exhillarating", "electrifying", "rousing"],
    "scary": ["spooky", "frightening", "terrifying", "horrific", "horrifying",
              "unnerving", "distressing"],
    "sexy": ["smutty", "smut", "erotic", "stimulating", "hot", "horny", "sexual",
             "explicit"],
}

CONTENT_WARNING_TAGS = {
    "sexual assault": ["sa", "sexual violence", "groping", "unwanted touching"],
    "death": ["dying", "fatality", "fatal", "mortality"],
    "violence": ["combat", "fighting", "physical assault", "assault"],
    "sexual content": ["sex", "sex scene", "sex scenes", "erotica", "erotic content",
                       "xxx", "x-rated", "smut", "pornography", "pornographic content"],
    "murder": ["killing"],
    "death of parent": ["patricide", "matricide", "parent death", "parental death"],
    "rape": ["grape", "non-consensual", "non-consensual sex", "noncon", "non con",
             "dubious consent", "dub con", "dubcon"],
    "blood": ["bleeding", "bleeding out"],
    "grief": ["mourning", "loss"],
    "child abuse": ["minor abuse", "parental abuse"],
    "injury/injury detail": ["injury", "injury detail", "detailed injuries", "injuries",
                             "physical trauma"],
    "child death": ["minor death"],
    "gore": ["grotesque injury", "gibbing", "viscera"],
    "racism": ["racial prejudice", "racial intolerance", "racial slurs",
               "white supremacy", "ethnic supremacy"],
    "emotional abuse": ["gaslighting", "bullying", "harassment"],
    "misogyny": ["gender discrimination"],
    "suicidal thoughts": ["suicidal ideation"],
    "medical content": ["surgery", "hospitalization"],
    "gun violence": ["shooting", "guns", "bullet wound", "bullet wounds", "firearms"],
    "homophobia": ["gay bashing", "queer bashing", "gay slurs", "lesbian slurs",
                    "queer slurs", "homophobic slurs"],
    "transphobia": ["misgendering", "gender erasure", "transphobic slurs"],
    "mental illness": ["insanity", "schizophrenia", "depression"],
    "war": ["military", "military content"],
    "confinement": ["imprisonment", "bondage", "claustrophobia", "restraints"],
    "drug abuse": ["addiction"]
}

def getCanonicalTag(tag:str, category:str) -> str | None:
    '''
    getCanonicalTag: Checks the given tag against potential synonyms in its category,
                     and returns the most common equivalent tag in Hardcover's database.
                     If no synonym is found, returns None.
    
    :param tag: The tag to check.
    :type tag: str
    :param category: The tag category, as returned by the LLM.
    :type category: str
    :return: The canonical tag if found, or None if not.
    :rtype: str | None
    '''
    match category:
        case "genre":
            synonyms = GENRE_SYNONYMS
        case "mood": 
            synonyms = MOOD_SYNONYMS
        case "content warnings": 
            synonyms = CONTENT_WARNING_TAGS
        case "title":
            return None
        case "authors":
            return None
        case "description":
            return None
        case _:
            raise ValueError(f'Unknown category: "{category}".')
        
    for canonical_tag in synonyms:
        if (canonical_tag in tag) or (tag in synonyms[canonical_tag]):
            return canonical_tag
    return None

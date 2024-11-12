def format_package_name(text:str)->str:
    """
    Format text to be used as a package name in CKAN
    
    """
    replacements={
        " ": "_", "'": "", "–": "-", ",": "-", ":": "--",
        "?": "", "&amp;": "-", "(": "", ")": "", "&": "-",
        ".": "", "'": ""
    }
    formatted=text.lower()
    for old, new in replacements.items():
        formatted=formatted.replace(old,new)
    
    return formatted[:100]
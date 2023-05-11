def userEntity(item) -> dict:
    return {
            "name" : item["name"],
            "email" : item["email"],
            "phone_number" : item ["phone_number"],
            "ref_geographic" : item ["ref_geographic"],
            
    }

def usersEntity (entity) -> list:
    [userEntity(item) for item in entity]
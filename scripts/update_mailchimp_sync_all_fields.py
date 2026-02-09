#!/usr/bin/env python3
"""
Helper functions to map all database fields to Mailchimp merge fields
Use this in sync_db_to_mailchimp.py
"""

from typing import Dict, Any, Optional
from datetime import date, datetime


def map_client_to_mailchimp_merge_fields(client_row: dict) -> dict:
    """
    Map all client database fields to Mailchimp merge fields
    
    Args:
        client_row: Client record from database
    
    Returns:
        Dict of Mailchimp merge fields
    """
    merge_fields = {}
    
    # ==== EXISTING FIELDS ====
    merge_fields["FNAME"] = client_row.get("first_name") or ""
    merge_fields["LNAME"] = client_row.get("last_name") or ""
    merge_fields["PHONE"] = client_row.get("phone") or ""
    merge_fields["MMERGE6"] = client_row.get("gender") or ""  # Gender
    merge_fields["MMERGE7"] = client_row.get("mobile") or ""  # Mobile
    merge_fields["MMERGE8"] = "Yes" if client_row.get("opt_in_phone") == 1 else "No"  # Phone Opt-In
    merge_fields["MMERGE16"] = client_row.get("custom_id") or ""  # Client ID
    merge_fields["MMERGE17"] = client_row.get("pabau_id") or ""  # Client System ID
    
    # Created info
    if client_row.get("created_date"):
        merge_fields["MMERGE12"] = format_date(client_row["created_date"])
    # Client Location (from Pabau location field)
    if client_row.get("location"):
        merge_fields["MMERGE11"] = client_row["location"]
    
    # ==== NEW BEHAVIORAL & VALUE FIELDS ====
    merge_fields["TOTSPEND"] = float(client_row.get("total_spend") or 0)
    merge_fields["VISITCNT"] = int(client_row.get("total_completed") or 0)
    merge_fields["CANCELCNT"] = int(client_row.get("total_cancelled") or 0)
    merge_fields["PENDINGCT"] = int(client_row.get("total_pending") or 0)
    
    if client_row.get("last_appt_date"):
        merge_fields["LASTVISIT"] = format_date(client_row["last_appt_date"])
    if client_row.get("next_appt_date"):
        merge_fields["NEXTAPPT"] = format_date(client_row["next_appt_date"])
    if client_row.get("last_appt_service"):
        merge_fields["LASTSVC"] = client_row["last_appt_service"]
    
    # ==== NEW DEMOGRAPHIC FIELDS ====
    if client_row.get("age"):
        merge_fields["AGE"] = int(client_row["age"])
    if client_row.get("salutation"):
        merge_fields["SALUTATION"] = client_row["salutation"]
    if client_row.get("dob"):
        merge_fields["DOB"] = format_date(client_row["dob"])
    
    # ==== NEW LOCATION FIELDS ====
    if client_row.get("mailing_city"):
        merge_fields["CITY"] = client_row["mailing_city"]
    if client_row.get("mailing_postal"):
        merge_fields["POSTCODE"] = client_row["mailing_postal"]
    if client_row.get("mailing_county"):
        merge_fields["COUNTY"] = client_row["mailing_county"]
    if client_row.get("mailing_country"):
        merge_fields["COUNTRY"] = client_row["mailing_country"]
    if client_row.get("mailing_street"):
        merge_fields["STREET"] = client_row["mailing_street"]
    if client_row.get("location"):
        merge_fields["LOCATION"] = client_row["location"]
    
    # ==== NEW ENGAGEMENT FIELDS ====
    merge_fields["ONLINEACT"] = "Yes" if client_row.get("online_account") == 1 else "No"
    merge_fields["ISACTIVE"] = "Yes" if client_row.get("is_active") == 1 else "No"
    merge_fields["CONFIRMCT"] = int(client_row.get("confirmed_appointments_count") or 0)
    
    # ==== NEW LIFECYCLE FIELDS ====
    if client_row.get("customer_stage"):
        merge_fields["CUSTSTAGE"] = client_row["customer_stage"]
    if client_row.get("days_since_created"):
        merge_fields["TENURE"] = int(client_row["days_since_created"])
    if client_row.get("last_updated_date"):
        merge_fields["LASTUPDT"] = format_date(client_row["last_updated_date"])
    
    # ==== NEW PREFERENCE FIELDS ====
    if client_row.get("preferred_practitioner"):
        merge_fields["PREFPRAC"] = client_row["preferred_practitioner"]
    if client_row.get("preferred_location"):
        merge_fields["PREFLOC"] = client_row["preferred_location"]
    if client_row.get("preferred_service"):
        merge_fields["PREFSVC"] = client_row["preferred_service"]
    
    # ==== ADDITIONAL OPT-INS ====
    merge_fields["OPTSMS"] = "Yes" if client_row.get("opt_in_sms") == 1 else "No"
    merge_fields["OPTPOST"] = "Yes" if client_row.get("opt_in_post") == 1 else "No"
    merge_fields["OPTNEWS"] = "Yes" if client_row.get("opt_in_newsletter") == 1 else "No"
    
    # ==== SOURCE FIELDS (expanded from JSONB) ====
    if client_row.get("primary_source_name"):
        merge_fields["LEADSOURCE"] = client_row["primary_source_name"]
    
    # All sources as comma-separated string
    all_sources = client_row.get("all_source_names") or []
    if all_sources and isinstance(all_sources, list):
        merge_fields["ALLSOURCES"] = ", ".join(all_sources)
    
    # ==== CUSTOM FIELDS (expanded from JSONB) ====
    if client_row.get("custom_owner"):
        merge_fields["CUSTOWNER"] = client_row["custom_owner"]
    if client_row.get("custom_landing_page"):
        merge_fields["LANDPAGE"] = client_row["custom_landing_page"]
    if client_row.get("custom_best_time_to_call"):
        merge_fields["CALLTIME"] = client_row["custom_best_time_to_call"]
    if client_row.get("custom_emergency_contact_name"):
        merge_fields["EMERGNAME"] = client_row["custom_emergency_contact_name"]
    if client_row.get("custom_emergency_contact_relation"):
        merge_fields["EMERGREL"] = client_row["custom_emergency_contact_relation"]
    if client_row.get("custom_emergency_contact_phone"):
        merge_fields["EMERGPH"] = client_row["custom_emergency_contact_phone"]
    if client_row.get("custom_gp_name"):
        merge_fields["GPNAME"] = client_row["custom_gp_name"]
    if client_row.get("custom_gp_surgery"):
        merge_fields["GPSURGERY"] = client_row["custom_gp_surgery"]
    
    # ==== INSURANCE FIELDS ====
    merge_fields["INSURANCE"] = "Yes" if client_row.get("has_insurance") == 1 else "No"
    if client_row.get("insurance_provider"):
        merge_fields["INSURER"] = client_row["insurance_provider"]
    
    # ==== FAMILY CONNECTIONS ====
    if client_row.get("family_connections"):
        merge_fields["FAMILYCNT"] = int(client_row["family_connections"])
    
    # ==== AVATAR ====
    if client_row.get("avatar"):
        merge_fields["AVATAR"] = client_row["avatar"]
    
    # ==== OWNER (from created_by_name if no separate owner field) ====
    if client_row.get("created_by_name"):
        merge_fields["OWNER"] = client_row["created_by_name"]
    if client_row.get("created_by_id"):
        merge_fields["OWNERID"] = int(client_row["created_by_id"])
    
    return merge_fields


def map_lead_to_mailchimp_merge_fields(lead_row: dict) -> dict:
    """
    Map all lead database fields to Mailchimp merge fields
    
    Args:
        lead_row: Lead record from database
    
    Returns:
        Dict of Mailchimp merge fields
    """
    merge_fields = {}
    
    # ==== BASIC INFO ====
    merge_fields["FNAME"] = lead_row.get("first_name") or ""
    merge_fields["LNAME"] = lead_row.get("last_name") or ""
    merge_fields["PHONE"] = lead_row.get("phone") or ""
    merge_fields["MMERGE7"] = lead_row.get("mobile") or ""  # Mobile
    merge_fields["MMERGE17"] = lead_row.get("pabau_id") or ""  # Lead System ID
    
    # ==== SALUTATION & DOB ====
    if lead_row.get("salutation"):
        merge_fields["SALUTATION"] = lead_row["salutation"]
    if lead_row.get("dob"):
        merge_fields["DOB"] = format_date(lead_row["dob"])
        # Calculate age from DOB
        try:
            dob_date = parse_date(lead_row["dob"])
            if dob_date:
                today = date.today()
                age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
                merge_fields["AGE"] = age
        except:
            pass
    
    # ==== LOCATION ====
    if lead_row.get("mailing_city"):
        merge_fields["CITY"] = lead_row["mailing_city"]
    if lead_row.get("mailing_postal"):
        merge_fields["POSTCODE"] = lead_row["mailing_postal"]
    if lead_row.get("mailing_county"):
        merge_fields["COUNTY"] = lead_row["mailing_county"]
    if lead_row.get("mailing_country"):
        merge_fields["COUNTRY"] = lead_row["mailing_country"]
    if lead_row.get("mailing_street"):
        merge_fields["STREET"] = lead_row["mailing_street"]
    
    # ==== LEAD-SPECIFIC FIELDS ====
    if lead_row.get("lead_status"):
        merge_fields["LEADSTAT"] = lead_row["lead_status"]
    if lead_row.get("source_name"):
        merge_fields["LEADSRC"] = lead_row["source_name"]
    if lead_row.get("converted_date"):
        merge_fields["CONVDATE"] = format_date(lead_row["converted_date"])
    if lead_row.get("pipeline_stage_name"):
        merge_fields["PIPELINE"] = lead_row["pipeline_stage_name"]
    if lead_row.get("deal_value"):
        merge_fields["DEALVAL"] = float(lead_row["deal_value"])
    
    # ==== OWNER ====
    if lead_row.get("owner_name"):
        merge_fields["OWNER"] = lead_row["owner_name"]
    if lead_row.get("owner_id"):
        merge_fields["OWNERID"] = int(lead_row["owner_id"])
    
    # ==== LOCATION NAME ====
    if lead_row.get("location_name"):
        merge_fields["LOCATION"] = lead_row["location_name"]
    
    # ==== DATES ====
    if lead_row.get("created_date"):
        merge_fields["MMERGE12"] = format_date(lead_row["created_date"])
    if lead_row.get("updated_date"):
        merge_fields["LASTUPDT"] = format_date(lead_row["updated_date"])
    
    # ==== CUSTOM FIELDS ====
    custom_fields = lead_row.get("custom_fields_data") or {}
    if isinstance(custom_fields, dict):
        custom_keys = list(custom_fields.keys())[:5]
        for i, key in enumerate(custom_keys, 1):
            merge_fields[f"CUSTOM{i}"] = str(custom_fields[key])
    
    return merge_fields


def generate_tags_from_client(client_row: dict) -> list:
    """
    Generate Mailchimp tags based on client data
    
    Args:
        client_row: Client record from database
    
    Returns:
        List of tag names to apply
    """
    tags = []
    
    # Base tag
    tags.append("Pabau Client")
    
    # Value-based tags
    total_spend = float(client_row.get("total_spend") or 0)
    if total_spend > 10000:
        tags.append("High Value")
    elif total_spend > 5000:
        tags.append("Medium Value")
    
    # Frequency-based tags
    visit_count = int(client_row.get("total_completed") or 0)
    if visit_count > 20:
        tags.append("Frequent Visitor")
    elif visit_count > 10:
        tags.append("Regular Visitor")
    
    # Stage-based tags
    stage = client_row.get("customer_stage")
    if stage:
        tags.append(f"Stage: {stage}")
    
    # Service-based tags
    last_service = client_row.get("last_appt_service")
    if last_service:
        tags.append(f"Service: {last_service}")
    
    # Location-based tags
    location = client_row.get("location") or client_row.get("preferred_location")
    if location:
        tags.append(f"Location: {location}")
    
    # Engagement tags
    if client_row.get("online_account") == 1:
        tags.append("Has Online Account")
    
    # Risk tags
    if stage == "Lapsed":
        tags.append("At Risk")
    
    return tags


def format_date(date_value: Any) -> str:
    """Format date for Mailchimp (YYYY-MM-DD)"""
    if not date_value:
        return ""
    
    if isinstance(date_value, str):
        # Already a string, try to parse and reformat
        try:
            dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except:
            return date_value[:10] if len(date_value) >= 10 else date_value
    elif isinstance(date_value, (date, datetime)):
        return date_value.strftime('%Y-%m-%d')
    
    return str(date_value)


def parse_date(date_value: Any) -> Optional[date]:
    """Parse date string to date object"""
    if not date_value:
        return None
    
    if isinstance(date_value, date):
        return date_value
    elif isinstance(date_value, datetime):
        return date_value.date()
    elif isinstance(date_value, str):
        try:
            return datetime.fromisoformat(date_value.replace('Z', '+00:00')).date()
        except:
            return None
    
    return None


# Example usage:
if __name__ == "__main__":
    # Example client record
    example_client = {
        "pabau_id": 12345,
        "email": "test@example.com",
        "first_name": "John",
        "last_name": "Doe",
        "total_spend": 15000,
        "total_completed": 25,
        "last_appt_date": "2024-11-01",
        "customer_stage": "VIP",
        "mailing_city": "Birmingham",
        "age": 35,
    }
    
    merge_fields = map_client_to_mailchimp_merge_fields(example_client)
    tags = generate_tags_from_client(example_client)
    
    print("Merge Fields:")
    for key, value in merge_fields.items():
        print(f"  {key}: {value}")
    
    print("\nTags:")
    for tag in tags:
        print(f"  - {tag}")


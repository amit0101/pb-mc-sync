"""
Data transformation utilities
Convert API responses to database schema format
"""
from datetime import datetime
from typing import List, Dict, Any, Optional


def transform_client_for_db(client_api_data: dict) -> dict:
    """
    Transform Pabau API client data to database schema
    
    Args:
        client_api_data: Raw client data from Pabau API
    
    Returns:
        Dict matching database schema
    """
    details = client_api_data.get('details', {})
    communications = client_api_data.get('communications', {})
    created = client_api_data.get('created', {})
    owner = created.get('owner', [{}])[0] if created.get('owner') else {}
    
    return {
        # Identifiers
        'pabau_id': details.get('id'),
        'custom_id': details.get('custom_id'),
        'email': communications.get('email'),
        
        # Basic info
        'first_name': details.get('first_name'),
        'last_name': details.get('last_name'),
        'salutation': details.get('salutation'),
        'gender': details.get('gender'),
        'dob': details.get('DOB'),
        'location': details.get('location'),
        'is_active': details.get('is_active', 1),
        
        # Communications
        'phone': communications.get('phone'),
        'mobile': communications.get('mobile'),
        'opt_in_email': communications.get('opt_in_email', 0),
        'opt_in_sms': communications.get('opt_in_sms', 0),
        'opt_in_phone': communications.get('opt_in_phone', 0),
        'opt_in_post': communications.get('opt_in_post', 0),
        'opt_in_newsletter': communications.get('opt_in_newsletter', 0),
        
        # Created info
        'created_date': created.get('created_date'),
        'created_by_name': owner.get('full_name'),
        'created_by_id': owner.get('created_by_id'),
    }


def parse_appointment_datetime(date_str: str, time_str: Optional[str] = None) -> Optional[str]:
    """
    Parse appointment date/time from Pabau format to ISO timestamp
    
    Args:
        date_str: Date in format "DD/MM/YYYY" or "DD/MM/YYYY HH:MM"
        time_str: Optional time in format "HH:MM"
    
    Returns:
        ISO format datetime string or None if parsing fails
    """
    if not date_str:
        return None
    
    try:
        # Handle combined date/time format: "23/10/2024 10:00"
        if ' ' in date_str and ':' in date_str:
            dt = datetime.strptime(date_str, '%d/%m/%Y %H:%M')
            return dt.isoformat()
        
        # Handle separate date and time
        if time_str:
            datetime_str = f"{date_str} {time_str}"
            dt = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M')
            return dt.isoformat()
        
        # Date only
        dt = datetime.strptime(date_str, '%d/%m/%Y')
        return dt.date().isoformat()
    except:
        return None


def transform_appointment_for_db(appointment_data: dict, client_pabau_id: int) -> dict:
    """
    Transform appointment data from Pabau client API to database schema
    
    Note: The /clients API returns a simplified appointments array.
    For full appointment details, use /appointments endpoint.
    
    Args:
        appointment_data: Appointment dict from client's appointments array
        client_pabau_id: Pabau ID of the client
    
    Returns:
        Dict matching appointments table schema
    """
    # Parse appointment_date which may contain time: "30/10/2025 09:00"
    appointment_date_str = appointment_data.get('appointment_date', '')
    
    # Extract date and time parts
    appointment_datetime = None
    appointment_date = None
    appointment_time = None
    
    if appointment_date_str:
        appointment_datetime = parse_appointment_datetime(appointment_date_str)
        # Parse date part only
        if ' ' in appointment_date_str:
            date_part = appointment_date_str.split(' ')[0]
            time_part = appointment_date_str.split(' ')[1] if len(appointment_date_str.split(' ')) > 1 else None
            try:
                appointment_date = datetime.strptime(date_part, '%d/%m/%Y').date().isoformat()
                appointment_time = time_part  # Keep as string "HH:MM"
            except:
                pass
    
    return {
        'client_pabau_id': client_pabau_id,
        'pabau_appointment_id': appointment_data.get('id'),  # Appointment ID from API
        'appointment_date': appointment_date,
        'appointment_time': appointment_time,
        'appointment_datetime': appointment_datetime,
        'service': appointment_data.get('service'),
        # Note: Most fields below are NOT in /clients appointments array
        # They would come from /appointments endpoint
        'location': None,  # Not in simple appointments array
        'duration': None,  # Not in simple appointments array
        'appointment_status': None,  # Not in simple appointments array
        'appt_with': None,  # Not in simple appointments array
        'created_by': None,  # Not in simple appointments array
        'created_date': None,  # Not in simple appointments array
        'cancellation_reason': None,  # Not in simple appointments array
    }


def transform_appointments_from_client(client_api_data: dict) -> List[Dict[str, Any]]:
    """
    Extract and transform all appointments from a client's API data
    
    Args:
        client_api_data: Raw client data from Pabau API
    
    Returns:
        List of appointment dicts ready for database insertion
    """
    client_pabau_id = client_api_data.get('details', {}).get('id')
    if not client_pabau_id:
        return []
    
    appointments = client_api_data.get('appointments', [])
    if not appointments:
        return []
    
    return [
        transform_appointment_for_db(appt, client_pabau_id) 
        for appt in appointments
    ]


def extract_custom_field(custom_fields: list, field_name: str):
    """
    Extract custom field value by name
    
    Note: The custom field "opt_in_email_lead" must be created in Pabau first!
    Field type: Integer (0 or 1)
    """
    if not custom_fields:
        return None
    
    for field in custom_fields:
        if isinstance(field, dict) and field.get('name') == field_name:
            value = field.get('value')
            # Convert to integer if it's a string
            if isinstance(value, str):
                try:
                    return int(value)
                except:
                    return 0 if value.lower() in ['0', 'false', 'no', ''] else 1
            return value
    
    return None


def transform_lead_for_db(lead_api_data: dict) -> dict:
    """
    Transform Pabau API lead data to database schema
    
    Args:
        lead_api_data: Raw lead data from Pabau API
    
    Returns:
        Dict matching database schema
    """
    owner = lead_api_data.get('owner', {})
    location = lead_api_data.get('location', {})
    dates = lead_api_data.get('dates', {})
    pipeline = lead_api_data.get('pipeline', {})
    stage = pipeline.get('stage', {}) if pipeline else {}
    custom_fields = lead_api_data.get('custom_fields', [])
    
    # Extract opt-in from custom field and convert to 0/1 format (like clients)
    opt_in_value = extract_custom_field(custom_fields, 'opt_in_email_lead')
    # Convert to 0 or 1, default to 0
    if opt_in_value is None:
        opt_in_email_mailchimp = 0
    elif isinstance(opt_in_value, int):
        opt_in_email_mailchimp = 1 if opt_in_value == 1 else 0
    elif isinstance(opt_in_value, str):
        # Handle string values like 'Opted In', 'true', '1', etc.
        opt_in_email_mailchimp = 1 if opt_in_value.lower() in ['opted in', 'true', '1', 'yes'] else 0
    else:
        opt_in_email_mailchimp = 0
    
    return {
        # Identifiers
        'pabau_id': lead_api_data.get('id'),
        'contact_id': lead_api_data.get('contact_id'),
        'email': lead_api_data.get('email'),
        
        # Basic info
        'salutation': lead_api_data.get('salutation'),
        'first_name': lead_api_data.get('first_name'),
        'last_name': lead_api_data.get('last_name'),
        'phone': lead_api_data.get('phone'),
        'mobile': lead_api_data.get('mobile'),
        'dob': lead_api_data.get('DOB'),
        
        # Address
        'mailing_street': lead_api_data.get('mailing_street'),
        'mailing_postal': lead_api_data.get('mailing_postal'),
        'mailing_city': lead_api_data.get('mailing_city'),
        'mailing_county': lead_api_data.get('mailing_county'),
        'mailing_country': lead_api_data.get('mailing_country'),
        
        # Status
        'is_active': lead_api_data.get('is_active', 1),
        'lead_status': lead_api_data.get('lead_status'),
        
        # Owner and location
        'owner_id': owner.get('id') if owner else None,
        'owner_name': owner.get('name') if owner else None,
        'location_id': location.get('id') if location else None,
        'location_name': location.get('name') if location else None,
        
        # Dates
        'created_date': dates.get('created_date') if dates else None,
        'updated_date': dates.get('updated_date') if dates else None,
        'converted_date': dates.get('converted_date') if dates else None,
        
        # Pipeline
        'pipeline_name': pipeline.get('name') if pipeline else None,
        'pipeline_stage_id': stage.get('pipeline_stage_id') if stage else None,
        'pipeline_stage_name': stage.get('pipeline_stage_name') if stage else None,
        
        # Deal
        'deal_value': lead_api_data.get('deal_value'),
        
        # Custom field for consent (0 or 1, matching client opt_in fields)
        'opt_in_email_mailchimp': opt_in_email_mailchimp,
    }


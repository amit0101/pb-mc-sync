"""
Data transformation utilities
Convert API responses to database schema format
"""
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import json
import hashlib


def calculate_age(dob_str: str) -> Optional[int]:
    """Calculate age from date of birth string"""
    if not dob_str:
        return None
    try:
        if isinstance(dob_str, str):
            dob = datetime.fromisoformat(dob_str.replace('Z', '+00:00')).date()
        else:
            dob = dob_str
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except:
        return None


def calculate_customer_stage(last_visit_date, total_completed: int, total_spend: float) -> str:
    """Calculate customer lifecycle stage"""
    if not last_visit_date:
        return "New"
    
    try:
        if isinstance(last_visit_date, str):
            last_visit = datetime.fromisoformat(last_visit_date.replace('Z', '+00:00')).date()
        else:
            last_visit = last_visit_date
        
        days_since_visit = (date.today() - last_visit).days
        
        # VIP criteria
        if total_spend and total_spend > 10000:
            return "VIP"
        if total_completed and total_completed > 20:
            return "VIP"
        
        # Active/Lapsed based on recency
        if days_since_visit <= 90:
            return "Active"
        elif days_since_visit <= 365:
            return "Lapsed"
        else:
            return "Inactive"
    except:
        return "Unknown"


def extract_custom_fields_as_json(custom_array: list) -> dict:
    """Convert Pabau custom fields array to JSON object"""
    if not custom_array:
        return {}
    
    result = {}
    for field in custom_array:
        if isinstance(field, dict):
            label = field.get('custom_field_label', '').strip()
            value = field.get('custom_field_value', '')
            if label:
                result[label] = value
    
    return result


def extract_custom_field_by_label(custom_array: list, label: str) -> str:
    """Extract a specific custom field value by label"""
    if not custom_array:
        return None
    
    for field in custom_array:
        if isinstance(field, dict):
            field_label = field.get('custom_field_label', '').strip()
            if field_label.lower() == label.lower():
                return field.get('custom_field_value', '')
    
    return None


def extract_sources(source_array: list) -> tuple:
    """
    Extract source information from source array
    
    Returns:
        tuple: (primary_source_name, primary_source_id, all_source_names)
    """
    if not source_array:
        return (None, None, [])
    
    primary_source_name = None
    primary_source_id = None
    all_source_names = []
    
    for source in source_array:
        if isinstance(source, dict):
            source_name = source.get('source_name')
            source_id = source.get('id')
            
            if source_name:
                all_source_names.append(source_name)
                
                # First source is primary
                if primary_source_name is None:
                    primary_source_name = source_name
                    primary_source_id = source_id
    
    return (primary_source_name, primary_source_id, all_source_names)


def calculate_data_hash(data: dict) -> str:
    """Calculate MD5 hash of data for change detection"""
    # Create stable string representation
    data_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()


def transform_client_for_db(client_api_data: dict) -> dict:
    """
    Transform Pabau API client data to database schema with ALL available fields
    
    Args:
        client_api_data: Raw client data from Pabau API
    
    Returns:
        Dict matching expanded database schema
    """
    details = client_api_data.get('details', {})
    communications = client_api_data.get('communications', {})
    created = client_api_data.get('created', {})
    owner = created.get('owner', [{}])[0] if created.get('owner') else {}
    address = client_api_data.get('address', {})
    client_insights = client_api_data.get('client_insights', [])
    insights = client_insights[0] if client_insights else {}
    custom_fields = client_api_data.get('custom', [])
    relationships = client_api_data.get('relationships', [])
    insurance = client_api_data.get('insurance', {})
    allergies = client_api_data.get('allergies', [])
    
    # Extract DOB and calculate age
    dob = details.get('DOB')
    age = calculate_age(dob)
    
    # Extract client insights metrics
    total_spend = float(insights.get('total_spend', 0)) if insights.get('total_spend') else 0
    total_completed = int(insights.get('total_completed', 0)) if insights.get('total_completed') else 0
    total_pending = int(insights.get('total_pending', 0)) if insights.get('total_pending') else 0
    total_cancelled = int(insights.get('total_cancelled', 0)) if insights.get('total_cancelled') else 0
    total_visits = int(insights.get('total_visits', 0)) if insights.get('total_visits') else 0
    total_noshow = int(insights.get('total_noshow', 0)) if insights.get('total_noshow') else 0
    last_appt_date = insights.get('last_appt_date')
    next_appt_date = insights.get('next_appt_date')
    first_visit_date = insights.get('first_visit')
    last_appt_service = insights.get('last_appt_service')
    next_appt_service = insights.get('next_appt_service')
    last_appt_with = insights.get('last_appt_with')
    next_appt_with = insights.get('next_appt_with')
    favorite_practitioner = insights.get('favorite_member')
    favorite_practitioner_id = int(insights.get('favorite_user_id')) if insights.get('favorite_user_id') else None
    avg_spend = float(insights.get('avg_spend', 0)) if insights.get('avg_spend') else 0
    account_balance = float(insights.get('account_balance', 0)) if insights.get('account_balance') else 0
    retail_sales = float(insights.get('retail_sales', 0)) if insights.get('retail_sales') else 0
    service_sales = float(insights.get('service_sales', 0)) if insights.get('service_sales') else 0
    appt_frequency = float(insights.get('appt_frequency', 0)) if insights.get('appt_frequency') else None
    review_score = float(insights.get('review_score', 0)) if insights.get('review_score') else None
    is_online_booking = int(insights.get('is_online_booking', 0)) if insights.get('is_online_booking') else 0
    
    # Calculate customer stage
    customer_stage = calculate_customer_stage(last_appt_date, total_completed, total_spend)
    
    # Calculate days since created
    days_since_created = None
    created_date = created.get('created_date')
    if created_date:
        try:
            created_dt = datetime.fromisoformat(created_date.replace('Z', '+00:00')).date()
            days_since_created = (date.today() - created_dt).days
        except:
            pass
    
    # Process custom fields
    custom_fields_json = extract_custom_fields_as_json(custom_fields)
    
    # Extract common custom fields as dedicated columns
    custom_owner = extract_custom_field_by_label(custom_fields, 'Owner')
    custom_landing_page = extract_custom_field_by_label(custom_fields, 'landing page')
    custom_best_time = extract_custom_field_by_label(custom_fields, 'best time to call')
    custom_emergency_name = extract_custom_field_by_label(custom_fields, 'Emergency Contact - Name')
    custom_emergency_relation = extract_custom_field_by_label(custom_fields, 'Emergency Contact - Relation')
    custom_emergency_phone = extract_custom_field_by_label(custom_fields, 'Next of kin contact number')
    custom_gp_name = extract_custom_field_by_label(custom_fields, 'GP Name')
    custom_gp_surgery = extract_custom_field_by_label(custom_fields, 'GP Surgery')
    
    # Extract source information
    source_raw = details.get('source', [])
    primary_source_name, primary_source_id, all_source_names = extract_sources(source_raw)
    
    # Process allergies
    allergies_list = [a.get('allergy') for a in allergies if isinstance(a, dict) and a.get('allergy')]
    has_allergies = 1 if allergies_list else 0
    
    # Process insurance
    has_insurance = 1 if insurance.get('insurer_name') else 0
    
    # Build complete client record
    client_record = {
        # Identifiers
        'pabau_id': details.get('id'),
        'custom_id': details.get('custom_id'),
        'email': communications.get('email'),
        
        # Basic info (existing)
        'first_name': details.get('first_name'),
        'last_name': details.get('last_name'),
        'salutation': details.get('salutation'),
        'gender': details.get('gender'),
        'dob': dob,
        'location': details.get('location'),
        'is_active': details.get('is_active', 1),
        
        # NEW: Additional details fields
        'avatar': details.get('avatar'),
        'source': json.dumps(details.get('source', [])) if details.get('source') else None,  # Store as JSONB
        'last_updated_date': details.get('last_updated_date'),
        'online_account': details.get('online_account', 0),
        'labels': details.get('labels', []),
        
        # Communications (existing)
        'phone': communications.get('phone'),
        'mobile': communications.get('mobile'),
        'opt_in_email': communications.get('opt_in_email', 0),
        'opt_in_sms': communications.get('opt_in_sms', 0),
        'opt_in_phone': communications.get('opt_in_phone', 0),
        'opt_in_post': communications.get('opt_in_post', 0),
        'opt_in_newsletter': communications.get('opt_in_newsletter', 0),
        
        # NEW: Address fields
        'mailing_street': address.get('mailing_street'),
        'other_street': address.get('other_street'),
        'mailing_city': address.get('mailing_city'),
        'mailing_county': address.get('mailing_county'),
        'mailing_country': address.get('mailing_country'),
        'mailing_postal': address.get('MailingPostal'),
        
        # Created info (existing)
        'created_date': created_date,
        'created_by_name': owner.get('full_name'),
        'created_by_id': owner.get('created_by_id'),
        
        # NEW: Client insights (business metrics)
        'total_spend': total_spend,
        'total_completed': total_completed,
        'total_pending': total_pending,
        'total_cancelled': total_cancelled,
        'total_visits': total_visits,
        'total_noshow': total_noshow,
        'next_appt_date': next_appt_date,
        'last_appt_date': last_appt_date,
        'first_visit_date': first_visit_date,
        'last_appt_service': last_appt_service,
        'next_appt_service': next_appt_service,
        'last_appt_with': last_appt_with,
        'next_appt_with': next_appt_with,
        'favorite_practitioner': favorite_practitioner,
        'favorite_practitioner_id': favorite_practitioner_id,
        'avg_spend': avg_spend,
        'account_balance': account_balance,
        'retail_sales': retail_sales,
        'service_sales': service_sales,
        'appt_frequency': appt_frequency,
        'review_score': review_score,
        'is_online_booking': is_online_booking,
        
        # NEW: Calculated fields
        'age': age,
        'customer_stage': customer_stage,
        'days_since_created': days_since_created,
        
        # NEW: Custom fields (JSONB + expanded columns)
        'custom_fields': custom_fields_json,
        'custom_owner': custom_owner,
        'custom_landing_page': custom_landing_page,
        'custom_best_time_to_call': custom_best_time,
        'custom_emergency_contact_name': custom_emergency_name,
        'custom_emergency_contact_relation': custom_emergency_relation,
        'custom_emergency_contact_phone': custom_emergency_phone,
        'custom_gp_name': custom_gp_name,
        'custom_gp_surgery': custom_gp_surgery,
        
        # NEW: Source fields (expanded from JSONB)
        'primary_source_name': primary_source_name,
        'primary_source_id': primary_source_id,
        'all_source_names': all_source_names,
        
        # NEW: Insurance
        'has_insurance': has_insurance,
        'insurance_provider': insurance.get('insurer_name'),
        'insurance_member_number': insurance.get('membership_number'),
        'insurance_cases': insurance.get('cases', 0),
        
        # NEW: Allergies
        'has_allergies': has_allergies,
        'allergies_list': allergies_list,
        
        # NEW: Relationships
        'family_connections': len(relationships),
        
        # NEW: Metadata
        'last_pabau_update': datetime.now().isoformat(),
    }
    
    # Calculate hash for change detection
    client_record['data_hash'] = calculate_data_hash(client_record)
    
    return client_record


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
    Transform appointment data to database schema - handles both simplified and full formats
    
    Note: The /clients API returns simplified appointments.
    For full details, this also handles /appointments endpoint format.
    
    Args:
        appointment_data: Appointment dict from API
        client_pabau_id: Pabau ID of the client
    
    Returns:
        Dict matching expanded appointments table schema
    """
    # Check if this is from /clients (simplified) or /appointments (full)
    dates = appointment_data.get('dates', {})
    details = appointment_data.get('details', {})
    service_array = appointment_data.get('service', [])
    communications = appointment_data.get('communications', {})
    
    # Handle simplified format from /clients
    appointment_date_str = appointment_data.get('appointment_date', '')
    
    # Extract date and time parts
    appointment_datetime = None
    appointment_date = None
    appointment_time = None
    start_date = None
    start_time = None
    end_time = None
    duration = None
    
    # If we have dates object (full appointment format)
    if dates:
        start_date = dates.get('start_date')
        start_time = dates.get('start_time')
        end_time = dates.get('end_time')
        duration = dates.get('duration')
        
        if start_date and start_time:
            appointment_datetime = f"{start_date} {start_time}"
            appointment_date = start_date
            appointment_time = start_time
    # Otherwise parse from simplified format
    elif appointment_date_str:
        appointment_datetime = parse_appointment_datetime(appointment_date_str)
        if ' ' in appointment_date_str:
            date_part = appointment_date_str.split(' ')[0]
            time_part = appointment_date_str.split(' ')[1] if len(appointment_date_str.split(' ')) > 1 else None
            try:
                appointment_date = datetime.strptime(date_part, '%d/%m/%Y').date().isoformat()
                appointment_time = time_part
                start_date = appointment_date
                start_time = appointment_time
            except:
                pass
    
    # Extract service info
    service_name = None
    service_price = None
    service_duration = None
    
    if service_array and len(service_array) > 0:
        service_obj = service_array[0]
        service_name = service_obj.get('service')
        service_price = service_obj.get('price')
        service_duration = service_obj.get('duration')
    else:
        service_name = appointment_data.get('service')  # Simplified format
    
    # Extract practitioner info
    practitioner = details.get('practitioner', {}) if details else {}
    practitioner_id = practitioner.get('practitioner_id')
    practitioner_name = practitioner.get('practitioner_name')
    
    # Extract location info
    location = details.get('location', {}) if details else {}
    location_id = location.get('id')
    location_name = location.get('name')
    
    # Extract creator info
    created_by = details.get('created_by', {}) if details else {}
    created_by_id = created_by.get('id')
    created_by_name = created_by.get('name')
    
    # Extract status and notes
    appointment_status = details.get('appointment_status') if details else None
    notes = details.get('notes') if details else None
    cancellation_reason = details.get('cancellation_reason') if details else None
    
    # Extract confirmation info
    sms_confirmation = communications.get('sms_confirmation', 0) if communications else 0
    email_confirmation = communications.get('email_confirmation', 0) if communications else 0
    confirmation_count = details.get('confirmations', 0) if details else 0
    
    # Extract created date
    created_date = details.get('create_date') if details else None
    
    return {
        # Core fields
        'client_pabau_id': client_pabau_id,
        'pabau_appointment_id': appointment_data.get('id'),
        
        # Date/time fields
        'appointment_date': appointment_date,
        'appointment_time': appointment_time,
        'appointment_datetime': appointment_datetime,
        'start_date': start_date,
        'start_time': start_time,
        'end_time': end_time,
        'duration': duration,
        
        # Service fields
        'service': service_name,
        'service_price': service_price,
        'service_duration': service_duration,
        
        # Location fields
        'location': location_name,  # Keep for backward compatibility
        'location_id': location_id,
        'location_name': location_name,
        
        # Practitioner fields
        'appt_with': practitioner_name,  # Keep for backward compatibility
        'practitioner_id': practitioner_id,
        'practitioner_name': practitioner_name,
        
        # Status fields
        'appointment_status': appointment_status,
        'notes': notes,
        'cancellation_reason': cancellation_reason,
        
        # Creator fields
        'created_by': created_by_name,  # Keep for backward compatibility
        'created_by_id': created_by_id,
        'created_by_name': created_by_name,
        'created_date': created_date,
        
        # Confirmation fields
        'sms_confirmation': sms_confirmation,
        'email_confirmation': email_confirmation,
        'confirmation_count': confirmation_count,
        
        # Metadata
        'last_updated': datetime.now().isoformat(),
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
    Transform Pabau API lead data to database schema with ALL available fields
    
    Args:
        lead_api_data: Raw lead data from Pabau API
    
    Returns:
        Dict matching expanded database schema
    """
    owner = lead_api_data.get('owner', {})
    location = lead_api_data.get('location', {})
    dates = lead_api_data.get('dates', {})
    pipeline = lead_api_data.get('pipeline', {})
    stage = pipeline.get('stage', {}) if pipeline else {}
    custom_fields = lead_api_data.get('custom_fields', [])
    source = lead_api_data.get('source', {})
    deal = lead_api_data.get('deal', {})
    
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
    
    # Process custom fields as JSON
    custom_fields_json = {}
    for field in custom_fields:
        if isinstance(field, dict):
            name = field.get('name', '').strip()
            value = field.get('value', '')
            if name:
                custom_fields_json[name] = value
    
    # Extract deal line items
    deal_line_items = deal.get('line_items', []) if deal else []
    
    lead_record = {
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
        
        # NEW: Source
        'source_id': source.get('id') if source else None,
        'source_name': source.get('name') if source else None,
        
        # NEW: Deal line items
        'deal_line_items': deal_line_items,
        
        # NEW: Custom fields
        'custom_fields_data': custom_fields_json,
        
        # Custom field for consent (0 or 1, matching client opt_in fields)
        'opt_in_email_mailchimp': opt_in_email_mailchimp,
        
        # NEW: Metadata
        'last_updated': datetime.now().isoformat(),
    }
    
    # Calculate hash for change detection
    lead_record['data_hash'] = calculate_data_hash(lead_record)
    
    return lead_record


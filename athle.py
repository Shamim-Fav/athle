import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import time
from typing import Optional, List

# Set page config
st.set_page_config(
    page_title="🏃 Athlefrance Competition Scraper",
    layout="centered",
    page_icon="🏃"
)

# ================== CONFIG ==================
BASE_URL = "https://www.athle.fr/bases/liste.aspx"
DETAIL_BASE_URL = "https://www.athle.fr/competitions/"
COMPETITIONS_PER_PAGE = 250
DEFAULT_DELAY = 1.0

# ================== HEADERS ==================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

# ================== SESSION MANAGEMENT ==================
@st.cache_resource(ttl=1800)
def get_session():
    """Create and cache a requests session."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

# ================== CORE SCRAPING FUNCTIONS ==================
def scrape_page(_session, params: dict, page: int):
    """Scrape a single page of competitions."""
    try:
        params_with_page = params.copy()
        params_with_page['frmposition'] = str(page)
        
        response = _session.get(BASE_URL, params=params_with_page, timeout=30)
        response.raise_for_status()
        
        competitions = parse_competitions(response.text, page)
        return competitions, True
    except Exception as e:
        st.error(f"Error scraping page {page}: {str(e)}")
        return [], False

def parse_competitions(html: str, page: int) -> List[dict]:
    """Parse competitions from HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    competitions = []
    
    # Find all competition rows
    rows = soup.find_all('tr', {'class': 'clignotant'})
    
    for row in rows:
        # Get all cells in the row
        cells = row.find_all('td')
        if len(cells) < 7:
            continue
        
        # Extract competition ID from the date link
        date_cell = cells[0]
        date_link = date_cell.find('a', title=True)
        
        competition_id = ""
        if date_link and date_link.get('title'):
            id_match = re.search(r'numéro : (\d+)', date_link.get('title'))
            competition_id = id_match.group(1) if id_match else ""
        
        # Extract date
        date = date_cell.get_text(strip=True) if date_cell else ""
        
        # Extract event name
        event = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        
        # Extract location
        location = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        
        # Extract type
        competition_type = cells[3].get_text(strip=True) if len(cells) > 3 else ""
        
        # Extract level
        level = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        
        # Extract detail URL - look in the last cell
        detail_url = ""
        if len(cells) >= 7:
            detail_link = cells[6].find('a')
            if detail_link and detail_link.get('href'):
                detail_url = detail_link.get('href')
                if detail_url.startswith('/'):
                    detail_url = f"https://www.athle.fr{detail_url}"
        
        competition = {
            'Competition_ID': competition_id,
            'Date': date,
            'Event': event,
            'Location': location,
            'Type': competition_type,
            'Level': level,
            'Detail_URL': detail_url,
            'Page': page,
            # Initialize detail page fields
            'Organizer_Name': '',
            'Organizer_Address': '',
            'Organizer_Phone': '',
            'Organizer_Email': '',
            'Organizer_Website': '',
            'Stadium_Address': '',
            'Competition_Code': '',
            'Contact_Person': '',
            'Events_List': '',
            'Events_Count': 0
        }
        competitions.append(competition)
    
    return competitions

def scrape_competitions(_session, params: dict, batch_mode: bool, batch_days: int,
                        progress_bar=None, status_text=None) -> List[dict]:
    """Scrape all competitions based on parameters."""
    all_competitions = []
    
    if batch_mode:
        # Parse dates
        start_dt = datetime.strptime(params['frmdate1'], "%Y-%m-%d")
        end_dt = datetime.strptime(params['frmdate2'], "%Y-%m-%d")
        current_start = start_dt
        batch_idx = 1
        
        while current_start <= end_dt:
            current_end = min(current_start + timedelta(days=batch_days), end_dt)
            batch_start_str = current_start.strftime("%Y-%m-%d")
            batch_end_str = current_end.strftime("%Y-%m-%d")
            
            batch_params = params.copy()
            batch_params['frmdate1'] = batch_start_str
            batch_params['frmdate2'] = batch_end_str
            
            if status_text:
                status_text.text(f"Processing batch {batch_idx}: {batch_start_str} to {batch_end_str}")
            
            page = 1
            while True:
                competitions, success = scrape_page(_session, batch_params, page)
                if not success or not competitions:
                    break
                
                all_competitions.extend(competitions)
                
                if status_text:
                    status_text.text(f"Batch {batch_idx}, Page {page}: {len(competitions)} competitions (Total: {len(all_competitions)})")
                
                if progress_bar:
                    progress_val = min(len(all_competitions) / 1000, 1.0)
                    progress_bar.progress(progress_val)
                
                # If we got fewer competitions than the page size, we're done with this batch
                if len(competitions) < COMPETITIONS_PER_PAGE:
                    break
                
                page += 1
                time.sleep(DEFAULT_DELAY)
            
            current_start = current_end + timedelta(days=1)
            batch_idx += 1
    else:
        # Normal mode
        page = 1
        while True:
            competitions, success = scrape_page(_session, params, page)
            if not success or not competitions:
                break
            
            all_competitions.extend(competitions)
            
            if status_text:
                status_text.text(f"Page {page}: {len(competitions)} competitions (Total: {len(all_competitions)})")
            
            if progress_bar:
                progress_val = min(len(all_competitions) / 1000, 1.0)
                progress_bar.progress(progress_val)
            
            if len(competitions) < COMPETITIONS_PER_PAGE:
                break
            
            page += 1
            time.sleep(DEFAULT_DELAY)
    
    return all_competitions

def scrape_detail_page(_session, url: str) -> Optional[dict]:
    """Scrape a single detail page."""
    try:
        response = _session.get(url, timeout=30)
        response.raise_for_status()
        return parse_detail_page(response.text)
    except Exception as e:
        st.warning(f"Error scraping detail page {url}: {str(e)[:100]}")
        return None

def parse_detail_page(html: str) -> dict:
    """Parse detail page HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    detail_data = {}
    
    # Extract organizer information
    info_section = soup.find('section', {'id': 'infoPratique'})
    if info_section:
        # Method 1: Look for email in mailto links
        mailto_links = info_section.find_all('a', href=lambda x: x and 'mailto:' in x)
        for link in mailto_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            if email:
                detail_data['Organizer_Email'] = email
                break
        
        # Method 2: Parse paragraph by paragraph
        paragraphs = info_section.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            
            if 'Nom de l’organisateur' in text or 'Nom de l\'organisateur' in text:
                detail_data['Organizer_Name'] = text.split(':', 1)[1].strip() if ':' in text else ''
            
            elif 'Adresse' in text and 'stade' not in text.lower():
                detail_data['Organizer_Address'] = text.split(':', 1)[1].strip() if ':' in text else ''
            
            elif 'Téléphone' in text:
                detail_data['Organizer_Phone'] = text.split(':', 1)[1].strip() if ':' in text else ''
            
            elif 'Email' in text and 'Organizer_Email' not in detail_data:
                email_text = text.split(':', 1)[1].strip() if ':' in text else ''
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', email_text)
                if email_match:
                    detail_data['Organizer_Email'] = email_match.group(0)
                elif email_text and email_text != '-':
                    detail_data['Organizer_Email'] = email_text
            
            elif 'Site internet' in text:
                website = text.split(':', 1)[1].strip() if ':' in text else ''
                if website and website != '-':
                    detail_data['Organizer_Website'] = website
            
            elif 'Adresse du stade' in text:
                detail_data['Stadium_Address'] = text.split(':', 1)[1].strip() if ':' in text else ''
    
    # Method 3: Fallback regex for email
    if 'Organizer_Email' not in detail_data or not detail_data['Organizer_Email']:
        if info_section:
            section_text = info_section.get_text()
            email_match = re.search(r'Email\s*[:\-]?\s*([\w\.-]+@[\w\.-]+\.\w+)', section_text, re.IGNORECASE)
            if email_match:
                detail_data['Organizer_Email'] = email_match.group(1).strip()
    
    # Extract competition code
    club_card = soup.find('div', class_='club-card')
    if club_card:
        card_text = club_card.get_text(separator='\n', strip=True)
        
        code_match = re.search(r'Code compétition\s*:\s*(\d+)', card_text)
        if code_match:
            detail_data['Competition_Code'] = code_match.group(1).strip()
        
        contact_match = re.search(r'Personnes à contacter.*\n*(.+)', card_text)
        if contact_match:
            detail_data['Contact_Person'] = contact_match.group(1).strip()[:100]
    
    # Extract events list
    events_section = soup.find('section', {'id': 'epreuves'})
    events_list = []
    if events_section:
        event_cards = events_section.find_all('div', class_='club-card')
        for card in event_cards:
            header = card.find('h3', class_='text-normal')
            if header:
                event_name = header.get_text(strip=True)
                events_list.append(event_name)
    
    detail_data['Events_List'] = '; '.join(events_list)
    detail_data['Events_Count'] = len(events_list)
    
    # Ensure all fields exist
    for field in ['Organizer_Name', 'Organizer_Address', 'Organizer_Phone', 
                 'Organizer_Email', 'Organizer_Website', 'Stadium_Address',
                 'Competition_Code', 'Contact_Person', 'Events_List']:
        if field not in detail_data:
            detail_data[field] = ''
    
    if 'Events_Count' not in detail_data:
        detail_data['Events_Count'] = 0
    
    return detail_data

def scrape_detail_pages(_session, competitions: List[dict], progress_bar=None, status_text=None) -> List[dict]:
    """Scrape detail pages for all competitions."""
    if not competitions:
        return competitions
    
    if status_text:
        status_text.text(f"Starting detail page scraping for {len(competitions)} competitions...")
    
    updated_competitions = []
    for idx, comp in enumerate(competitions):
        if not comp.get('Detail_URL'):
            updated_competitions.append(comp)
            continue
            
        if status_text:
            status_text.text(f"Scraping detail {idx + 1}/{len(competitions)}: {comp['Event'][:30]}...")
        
        detail_data = scrape_detail_page(_session, comp['Detail_URL'])
        if detail_data:
            comp.update(detail_data)
        
        updated_competitions.append(comp)
        
        if progress_bar:
            progress_val = (idx + 1) / len(competitions)
            progress_bar.progress(progress_val)
        
        time.sleep(DEFAULT_DELAY)
    
    return updated_competitions

# ================== DATA PROCESSING ==================
@st.cache_data
def create_final_dataframe(competitions: List[dict]) -> pd.DataFrame:
    """Create final DataFrame with proper column ordering."""
    if not competitions:
        return pd.DataFrame()
    
    df = pd.DataFrame(competitions)
    
    # Define column order
    column_order = [
        'Competition_ID',
        'Date',
        'Event',
        'Location',
        'Type',
        'Level',
        'Organizer_Name',
        'Organizer_Address',
        'Organizer_Phone',
        'Organizer_Email',
        'Organizer_Website',
        'Stadium_Address',
        'Competition_Code',
        'Contact_Person',
        'Events_List',
        'Events_Count',
        'Detail_URL',
        'Page'
    ]
    
    # Reorder columns
    existing_cols = [col for col in column_order if col in df.columns]
    df = df[existing_cols]
    
    return df

@st.cache_data
def convert_to_csv(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to CSV with proper encoding."""
    return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')

@st.cache_data
def convert_to_excel(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Excel."""
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Competitions')
        worksheet = writer.sheets['Competitions']
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            worksheet.column_dimensions[chr(65 + col_idx)].width = min(column_length + 2, 50)
    
    return output.getvalue()

# ================== STREAMLIT UI ==================
st.title("🏃 Athlefrance Competition Scraper")
st.markdown("Scrape athletic competitions from athle.fr with detailed information extraction.")

# Create tabs for better organization
tab1, tab2 = st.tabs(["🏁 Scrape Competitions", "ℹ️ About"])

with tab1:
    # Input parameters in columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        season = st.text_input("Season", value="2026")
        batch_days = st.number_input("Days per batch", min_value=1, max_value=365, value=30)
    
    with col2:
        start_date = st.text_input("Start Date (YYYY-MM-DD)", value="2025-12-21")
        end_date = st.text_input("End Date (YYYY-MM-DD)", value="2026-01-31")
    
    with col3:
        batch_mode = st.checkbox("Batch Mode", value=False)
        scrape_details = st.checkbox("Scrape Detail Pages", value=True)
    
    # Base parameters
    base_params = {
        'frmpostback': 'true',
        'frmbase': 'calendrier',
        'frmmode': '1',
        'frmespace': '0',
        'frmsaisonffa': season,
        'frmdate1': start_date,
        'frmdate2': end_date,
        'frmposition': '1'
    }
    
    # Scrape button
    if st.button("🚀 Start Scraping", type="primary", use_container_width=True):
        # Validate dates
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            st.error("Invalid date format. Please use YYYY-MM-DD.")
            st.stop()
        
        # Create session
        session = get_session()
        
        # Initialize progress indicators
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with st.spinner("Scraping competitions..."):
            try:
                # Scrape competitions
                competitions = scrape_competitions(
                    session, base_params, batch_mode, batch_days,
                    progress_bar, status_text
                )
                
                if not competitions:
                    st.warning("No competitions found with the given criteria.")
                    st.stop()
                
                st.success(f"Found {len(competitions)} competitions!")
                
                # Display some debug info
                st.info(f"Sample Detail URLs (first 5):")
                for i, comp in enumerate(competitions[:5]):
                    st.write(f"{i+1}. {comp.get('Detail_URL', 'No URL')}")
                
                # Scrape detail pages if requested
                if scrape_details and competitions:
                    progress_bar.empty()
                    status_text.empty()
                    
                    detail_progress = st.progress(0)
                    detail_status = st.empty()
                    
                    with st.spinner("Scraping detail pages..."):
                        competitions = scrape_detail_pages(
                            session, competitions, detail_progress, detail_status
                        )
                    
                    detail_progress.empty()
                    detail_status.empty()
                    st.success("Detail pages scraped successfully!")
                
                # Create and display DataFrame
                df = create_final_dataframe(competitions)
                
                # Show summary
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Competitions", len(df))
                with col2:
                    st.metric("With Email", df['Organizer_Email'].str.contains('@').sum())
                with col3:
                    st.metric("With Events", df['Events_Count'].sum())
                
                # Display data
                st.subheader("📊 Competition Data Preview")
                st.dataframe(
                    df.head(100),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Detail_URL": st.column_config.LinkColumn("Detail URL"),
                        "Organizer_Email": st.column_config.TextColumn("Email", width="medium"),
                        "Events_List": st.column_config.TextColumn("Events", width="large")
                    }
                )
                
                # Download section
                st.subheader("📥 Download Data")
                
                # Create two columns for download buttons
                col1, col2 = st.columns(2)
                
                with col1:
                    csv_data = convert_to_csv(df)
                    st.download_button(
                        label="📄 Download as CSV",
                        data=csv_data,
                        file_name=f"athle_competitions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with col2:
                    excel_data = convert_to_excel(df)
                    st.download_button(
                        label="📊 Download as Excel",
                        data=excel_data,
                        file_name=f"athle_competitions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                
                # Show detailed statistics
                with st.expander("📈 Detailed Statistics"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Competition Types:**")
                        type_counts = df['Type'].value_counts()
                        if not type_counts.empty:
                            st.dataframe(type_counts, use_container_width=True)
                    
                    with col2:
                        st.write("**Competition Levels:**")
                        level_counts = df['Level'].value_counts()
                        if not level_counts.empty:
                            st.dataframe(level_counts, use_container_width=True)
                
                # Show sample of detailed data
                with st.expander("🔍 Sample Detailed Information"):
                    if not df.empty:
                        sample = df[['Event', 'Organizer_Name', 'Organizer_Email', 'Stadium_Address', 'Events_Count']].head(5)
                        st.dataframe(sample, use_container_width=True, hide_index=True)
                
            except Exception as e:
                st.error(f"An error occurred during scraping: {str(e)}")
                st.exception(e)

with tab2:
    st.header("ℹ️ About This Tool")
    
    st.markdown("""
    ### Features
    
    🏃 **Two-Phase Scraping:**
    1. **First Phase:** Scrapes competition listings from athle.fr
    2. **Second Phase:** Optionally visits each competition's detail page for complete information
    
    📊 **Data Extracted:**
    - Basic competition info (Date, Event, Location, Type, Level)
    - Organizer details (Name, Address, Phone, Email, Website)
    - Stadium address
    - Competition code
    - Contact person
    - Event list and count
    
    ⚙️ **Advanced Features:**
    - Batch mode for large date ranges
    - Progress tracking
    - Error handling
    - Multiple download formats (CSV & Excel)
    
    ### Usage Tips
    
    1. **Start Date & End Date:** Use YYYY-MM-DD format
    2. **Batch Mode:** Recommended for large date ranges (>30 days)
    3. **Scrape Detail Pages:** Check this to get complete organizer information
    4. **Delay:** Built-in delays to prevent rate limiting
    
    ### Data Privacy
    
    This tool only scrapes publicly available information from athle.fr.
    Use responsibly and in accordance with the website's terms of service.
    
    ### Technical Details
    
    - Built with Python, Streamlit, BeautifulSoup, and pandas
    - Uses requests with proper headers and delays
    - Triple-method email extraction for reliability
    - Real-time progress updates
    
    ---
    
    *For issues or feature requests, please check the script's functionality.*
    """)

# Add some styling
st.markdown("""
<style>
    .stButton > button {
        width: 100%;
    }
    .stDownloadButton > button {
        width: 100%;
    }
    div[data-testid="stExpander"] div[role="button"] p {
        font-size: 1.1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

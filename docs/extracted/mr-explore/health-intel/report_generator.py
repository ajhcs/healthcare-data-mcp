"""Health System Intelligence Report Generator.

Takes collected health system data (JSON) and generates a comprehensive
50-150 page PDF report using Claude API for analysis and WeasyPrint for
PDF rendering.

A product of Open-Informatics.org
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Any

import anthropic
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

# Report sections in order
REPORT_SECTIONS = [
    "executive_summary",
    "organization_overview",
    "leadership_governance",
    "financial_performance",
    "clinical_quality",
    "patient_experience",
    "pricing_analysis",
    "provider_network",
    "clinical_trials_research",
    "market_position",
    "strategic_outlook",
]

SECTION_TITLES = {
    "executive_summary": "Executive Summary",
    "organization_overview": "Organization Overview",
    "leadership_governance": "Leadership & Governance",
    "financial_performance": "Financial Performance",
    "clinical_quality": "Clinical Quality & Safety",
    "patient_experience": "Patient Experience",
    "pricing_analysis": "Pricing & Cost Analysis",
    "provider_network": "Provider Network & Workforce",
    "clinical_trials_research": "Research & Clinical Trials",
    "market_position": "Market Position & Competitive Landscape",
    "strategic_outlook": "Strategic Outlook",
}

SECTION_PROMPTS = {
    "executive_summary": """Write a comprehensive executive summary (2-3 pages) for {system_name}.

This should be a high-level overview covering:
- Key facts: system size, location, type, academic affiliation
- Financial highlights: total revenue, operating margin trend, notable changes
- Quality standing: overall CMS star rating, key quality indicators
- Strategic position: market standing, competitive advantages, challenges
- Key takeaways for stakeholders

Use specific numbers and data from the collected data. Write in a professional analyst tone.
Format as HTML with <h2>, <h3>, <p>, <ul>, <li>, <table>, <strong> tags. Use tables for key metrics.""",

    "organization_overview": """Write a detailed organization overview (5-8 pages) for {system_name}.

Cover:
- History and founding (research what you know about this system)
- Mission and values
- System structure: hospitals, clinics, facilities, geographic coverage
- Academic affiliations and teaching programs
- Key service lines and centers of excellence
- Recent mergers, acquisitions, or partnerships
- Governance structure (board composition if known)

Use data from the system profile and NPI data to describe the organizational footprint.
Include a table of affiliated facilities from the NPI data.
Format as HTML with appropriate headings, paragraphs, tables, and lists.""",

    "leadership_governance": """Write a leadership and governance analysis (3-5 pages) for {system_name}.

Cover:
- CEO and C-suite leadership team (research current leadership)
- Executive compensation from 990 data (list top compensated officers)
- Board of Directors composition
- Medical staff leadership
- Key physician leaders and department chairs (from NPI data if available)
- Leadership changes and succession

Use the 990 filing data for executive compensation details.
Format as HTML with tables for compensation data.""",

    "financial_performance": """Write a comprehensive financial analysis (8-12 pages) for {system_name}.

Cover:
- Revenue trends (5-year if available from 990 data)
- Operating expenses and margins
- Revenue breakdown: patient service revenue, contributions, investment income
- Total assets and net assets trend
- Liabilities and debt position
- Employee counts and staffing trends
- Charity care and community benefit
- Comparison to peer institutions
- Financial outlook and risks

Create HTML tables showing year-over-year financial data.
Include trend analysis with specific dollar amounts.
Use the 990 filing data extensively — extract every financial metric available.
Format as HTML with detailed tables, trend commentary, and charts descriptions.""",

    "clinical_quality": """Write a detailed clinical quality and safety analysis (8-10 pages) for {system_name}.

Cover:
- Overall CMS star rating and component scores
- Mortality measures: heart attack, heart failure, pneumonia, COPD, stroke, CABG
- Complication rates and safety measures
- Healthcare-associated infections: CLABSI, CAUTI, SSI, MRSA, C.diff
- Readmission rates and HRRP penalties
- Timely and effective care measures
- Comparison to national averages (better/same/worse)
- Quality improvement initiatives

Use ALL of the CMS quality data provided. Create tables comparing hospital rates to national benchmarks.
Categorize measures as better than, same as, or worse than expected.
Format as HTML with detailed data tables.""",

    "patient_experience": """Write a patient experience analysis (4-6 pages) for {system_name}.

Cover:
- Overall HCAHPS rating and star rating
- Dimension-by-dimension analysis:
  * Communication with nurses
  * Communication with doctors
  * Responsiveness of hospital staff
  * Communication about medicines
  * Cleanliness and quietness
  * Discharge information
  * Care transition
  * Overall hospital rating
  * Willingness to recommend
- Top and bottom scores relative to state/national averages
- Survey response rates
- Trends if available
- Recommendations for improvement

Use the HCAHPS data extensively. Create tables showing each measure.
Format as HTML with data tables and narrative analysis.""",

    "pricing_analysis": """Write a pricing and cost analysis (5-8 pages) for {system_name}.

Cover:
- Medicare spending per beneficiary vs national average
- Spending by claim type (if available)
- Cost efficiency measures from CMS data
- Price transparency context (CMS machine-readable file requirements)
- Regional pricing context for the Philadelphia/South Jersey market
- Payer mix implications (Medicare, Medicaid, commercial)
- Out-of-pocket cost implications for patients
- Value-based care positioning

Use the Medicare spending data from CMS.
Discuss the hospital's compliance with price transparency requirements.
Format as HTML with comparison tables.""",

    "provider_network": """Write a provider network and workforce analysis (4-6 pages) for {system_name}.

Cover:
- Total number of affiliated providers (from NPI data)
- Specialty distribution and mix
- Key service line coverage
- Provider-to-population ratios in service area
- Workforce challenges and recruitment
- Graduate medical education (residency programs)
- Physician alignment model (employed vs. independent)
- Telehealth and virtual care capabilities

Use the NPI taxonomy data to show specialty distribution.
Create a table of top specialties by provider count.
Format as HTML with data tables.""",

    "clinical_trials_research": """Write a research and clinical trials analysis (4-6 pages) for {system_name}.

Cover:
- Active clinical trials (use ClinicalTrials.gov context)
- Research focus areas and centers
- Academic affiliations and medical school partnership
- Notable publications and research output
- Research funding sources
- Translational research initiatives
- Key principal investigators
- Technology and innovation programs

Discuss the system's research profile in the context of its academic mission.
Format as HTML with organized sections.""",

    "market_position": """Write a market position and competitive landscape analysis (5-8 pages) for {system_name}.

Cover:
- Market definition: Philadelphia/South Jersey healthcare market
- Key competitors: Penn Medicine, Main Line Health, Virtua, Inspira, etc.
- Market share estimates based on bed count, revenue, patient volume
- Competitive advantages and differentiators
- Recent M&A activity in the market
- Managed care positioning
- Geographic coverage and access points
- Population health trends in the service area
- Regulatory environment (CON, state mandates)
- Payer landscape in PA and NJ

Use the financial and quality data to position the system relative to competitors.
Format as HTML with competitor comparison tables.""",

    "strategic_outlook": """Write a strategic outlook and SWOT analysis (4-6 pages) for {system_name}.

Cover:
- SWOT Analysis:
  * Strengths (from quality, financial, and market data)
  * Weaknesses (from quality gaps, financial challenges)
  * Opportunities (market trends, expansion, partnerships)
  * Threats (competitive, regulatory, financial risks)
- 3-5 year strategic priorities
- Key performance indicators to watch
- Recommendations for stakeholders
- Risk factors and mitigation strategies
- Industry trends affecting the system

Synthesize ALL data from previous sections into a forward-looking analysis.
Format as HTML with a SWOT table and structured recommendations.""",
}


class ReportGenerator:
    def __init__(self, data_path: str, output_dir: str = "output"):
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        with open(self.data_path) as f:
            self.data = json.load(f)

        self.system_name = self.data["system_profile"]["name"]
        self.short_name = self.data["system_profile"]["short_name"]
        self.client = anthropic.Anthropic()

        # Template environment with autoescape enabled
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(default=True),
        )

    def _prepare_section_context(self, section: str) -> str:
        """Prepare the data context for a specific section."""
        context_parts = []

        # Always include system profile
        context_parts.append(
            f"## System Profile\n```json\n{json.dumps(self.data['system_profile'], indent=2)}\n```"
        )

        # Section-specific data
        if section in ("executive_summary", "strategic_outlook", "market_position"):
            # These sections need everything
            for key in ("cms_quality_data", "financial_data", "provider_data"):
                if key in self.data:
                    # Truncate large datasets to fit context
                    data_str = json.dumps(self.data[key], indent=2, default=str)
                    if len(data_str) > 50000:
                        data_str = data_str[:50000] + "\n... (truncated)"
                    context_parts.append(f"## {key}\n```json\n{data_str}\n```")

        elif section in ("clinical_quality", "patient_experience"):
            cms = self.data.get("cms_quality_data", {})
            data_str = json.dumps(cms, indent=2, default=str)
            if len(data_str) > 80000:
                data_str = data_str[:80000] + "\n... (truncated)"
            context_parts.append(f"## CMS Quality Data\n```json\n{data_str}\n```")

        elif section == "financial_performance":
            fin = self.data.get("financial_data", {})
            data_str = json.dumps(fin, indent=2, default=str)
            context_parts.append(f"## 990 Financial Data\n```json\n{data_str}\n```")

        elif section == "leadership_governance":
            fin = self.data.get("financial_data", {})
            data_str = json.dumps(fin, indent=2, default=str)
            context_parts.append(f"## 990 Financial Data (for compensation)\n```json\n{data_str}\n```")

        elif section == "provider_network":
            prov = self.data.get("provider_data", {})
            data_str = json.dumps(prov, indent=2, default=str)
            if len(data_str) > 60000:
                data_str = data_str[:60000] + "\n... (truncated)"
            context_parts.append(f"## Provider/NPI Data\n```json\n{data_str}\n```")

        elif section == "pricing_analysis":
            cms = self.data.get("cms_quality_data", {})
            # Just spending data
            spending = {}
            if isinstance(cms, dict) and "cms_data" in cms:
                for k in ("medicare_spending", "timely_effective_care"):
                    if k in cms["cms_data"]:
                        spending[k] = cms["cms_data"][k]
            context_parts.append(
                f"## CMS Spending Data\n```json\n{json.dumps(spending, indent=2, default=str)}\n```"
            )

        elif section == "organization_overview":
            prov = self.data.get("provider_data", {})
            data_str = json.dumps(prov, indent=2, default=str)
            if len(data_str) > 40000:
                data_str = data_str[:40000] + "\n... (truncated)"
            context_parts.append(f"## Provider/NPI Data\n```json\n{data_str}\n```")

        return "\n\n".join(context_parts)

    async def generate_section(self, section: str) -> str:
        """Generate a single report section using Claude API."""
        prompt_template = SECTION_PROMPTS.get(section)
        if not prompt_template:
            return f"<p>Section '{section}' not implemented.</p>"

        prompt = prompt_template.format(system_name=self.system_name)
        context = self._prepare_section_context(section)

        print(f"  Generating: {SECTION_TITLES.get(section, section)}...")

        message = self.client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8000,
            messages=[
                {
                    "role": "user",
                    "content": f"""You are a healthcare industry analyst writing a professional intelligence report on {self.system_name}.

Here is the collected data for this health system:

{context}

{prompt}

IMPORTANT:
- Use ONLY real data from the provided datasets. Do not fabricate numbers.
- When data is not available, state "Data not available" rather than guessing.
- Write in a professional, analytical tone suitable for a paid research report.
- Include specific numbers, percentages, and comparisons throughout.
- Format output as clean HTML (no <html>, <head>, <body> tags — just content tags).
- Use <table> with <thead>/<tbody> for data tables. Include border styling inline.
- Every table should have class="data-table" for consistent styling.
- Aim for {5 if section == 'executive_summary' else 8}-{8 if section == 'executive_summary' else 15} printed pages of content.""",
                }
            ],
        )

        content = message.content[0].text
        print(f"    Done ({message.usage.output_tokens} tokens)")
        return content

    async def generate_all_sections(self) -> dict[str, str]:
        """Generate all report sections. Returns section_key -> HTML content."""
        sections = {}
        for section in REPORT_SECTIONS:
            try:
                html = await self.generate_section(section)
                sections[section] = html
            except Exception as e:
                print(f"    ERROR generating {section}: {e}")
                sections[section] = f'<div class="error"><p>Error generating this section: {e}</p></div>'
        return sections

    def render_pdf(self, sections: dict[str, str]) -> Path:
        """Render all sections into a single PDF report."""
        try:
            from weasyprint import HTML
        except ImportError:
            print("WeasyPrint not installed. Saving as HTML instead.")
            return self._render_html(sections)

        html_content = self._build_full_html(sections)
        pdf_path = self.output_dir / f"{self.data['metadata']['system_key']}_report.pdf"

        HTML(string=html_content).write_pdf(str(pdf_path))
        print(f"\n  PDF saved to: {pdf_path}")
        print(f"  File size: {pdf_path.stat().st_size / 1024 / 1024:.1f} MB")
        return pdf_path

    def _render_html(self, sections: dict[str, str]) -> Path:
        """Render as HTML file (fallback when WeasyPrint is unavailable)."""
        html_content = self._build_full_html(sections)
        html_path = self.output_dir / f"{self.data['metadata']['system_key']}_report.html"

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"\n  HTML saved to: {html_path}")
        print(f"  File size: {html_path.stat().st_size / 1024:.1f} KB")
        return html_path

    def _build_full_html(self, sections: dict[str, str]) -> str:
        """Build the complete HTML document with all sections."""
        template = self.jinja_env.get_template("report.html")
        today = datetime.now().strftime("%B %d, %Y")

        section_data = []
        for i, key in enumerate(REPORT_SECTIONS):
            section_data.append({
                "number": i + 1,
                "title": SECTION_TITLES.get(key, key),
                "key": key,
                # Content is trusted HTML from Claude API — mark safe for Jinja2
                "content": Markup(sections.get(key, "<p>Content not available.</p>")),
            })

        return template.render(
            system_name=self.system_name,
            short_name=self.short_name,
            report_date=today,
            sections=section_data,
            metadata=self.data.get("metadata", {}),
        )


async def generate_report(data_path: str, output_dir: str = "output") -> Path:
    """Generate a complete report from collected data."""
    gen = ReportGenerator(data_path, output_dir)

    print(f"\nGenerating report for: {gen.system_name}")
    print(f"Data source: {data_path}")
    print(f"Output directory: {output_dir}")
    print(f"Sections: {len(REPORT_SECTIONS)}")
    print()

    sections = await gen.generate_all_sections()
    path = gen.render_pdf(sections)

    return path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python report_generator.py <data_json_path> [output_dir]")
        sys.exit(1)

    data_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"

    asyncio.run(generate_report(data_path, output_dir))

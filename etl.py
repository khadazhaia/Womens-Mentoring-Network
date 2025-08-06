import pandas as pd


def extract_excel():
    """
    Load Excel files
    """
    demographics = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/demographics.xlsx")
    intake = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/intake.xlsx")
    meetings = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/meetings.xlsx")
    phone_calls = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/phone_calls.xlsx")
    salaries = pd.read_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/salaries.xlsx")
    return intake, demographics, salaries, phone_calls, meetings


def client_id(df):
    """
    - Rename Columns, Clean & Fill Missing Values
    - Start with First Name
    - If duplicate: add MI
    - If still duplicate: add Telephone
    - If still duplicate: add index
    - Finally map to numbers
    """
    # Rename Columns to match across files
    if "Name" in df.columns:
        df = df.rename(columns={"Name": "First Name"})

    # Clean and fill missing values before converting to string
    if "First Name" in df.columns:
        df["First Name"] = df["First Name"].fillna("").astype(str).str.strip()
    if "MI" in df.columns:
        df["MI"] = df["MI"].fillna("").astype(str).str.strip()
    if "Telephone" in df.columns:
        df["Telephone"] = df["Telephone"].fillna("").astype(str).str.strip()

    # Start with First Name
    if "First Name" in df.columns and df["First Name"].any():
        df["ClientID"] = df["First Name"]

        # Find duplicates
        duplicates = df[df["ClientID"].duplicated(keep=False)]

        if not duplicates.empty:
            # Add MI where available (skip if blank)
            if "MI" in df.columns:
                mask = df["ClientID"].isin(duplicates["ClientID"]) & (df["MI"] != "")
                df.loc[mask, "ClientID"] = (
                    df.loc[mask, "First Name"] + "_" + df.loc[mask, "MI"]
                ).str.strip("_")

            # Re-check duplicates
            duplicates = df[df["ClientID"].duplicated(keep=False)]

            if not duplicates.empty:
                # Add Telephone where available (skip if blank)
                if "Telephone" in df.columns:
                    mask = df["ClientID"].isin(duplicates["ClientID"]) & (df["Telephone"] != "")
                    df.loc[mask, "ClientID"] = (
                        df.loc[mask, "ClientID"] + "_" + df.loc[mask, "Telephone"]
                    ).str.strip("_")

                # Re-check duplicates
                duplicates = df[df["ClientID"].duplicated(keep=False)]

                if not duplicates.empty:
                    # Append index
                    df.loc[duplicates.index, "ClientID"] = (
                        df.loc[duplicates.index, "ClientID"] + "_" +
                        df.loc[duplicates.index].groupby("ClientID").cumcount().astype(str))

    elif "Telephone" in df.columns and df["Telephone"].any():
        df["ClientID"] = df["Telephone"]

    else:
        print("No 'First Name' or 'Telephone' column found; skipping ClientID creation.")

    # Map to numeric IDs while preserving uniqueness
    unique_ids = {val: i+1 for i, val in enumerate(df["ClientID"].unique())}
    df["ClientID"] = df["ClientID"].map(unique_ids)

    return df


def transform_data(intake, demographics, salaries, phone_calls, meetings):
    """
    - Apply transformations and create ClientIDs
    """
    # Create ClientIDs
    intake = client_id(intake)
    demographics = client_id(demographics)
    salaries = client_id(salaries)
    phone_calls = client_id(phone_calls)
    meetings = client_id(meetings)

    return intake, demographics, salaries, phone_calls, meetings


def merge_export(intake, demographics, salaries, phone_calls, meetings):
    """
    - Base = demographics (has First Name + Telephone)
    - Merge intake, meetings, salaries by ClientID
    - Merge phone_calls by Telephone
    """
    # Drop 'First Name' from all files except demographics
    for df in [intake, salaries, phone_calls, meetings]:
        for col in ["First Name", "MI", "Last Name"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

    # Start from demographics
    client_data = demographics.merge(intake, on="ClientID", how="outer", suffixes=("_Demo", "_Intake"))
    client_data = client_data.merge(meetings, on="ClientID", how="outer", suffixes=("", "_Meeting"))
    client_data = client_data.merge(salaries, on="ClientID", how="outer", suffixes=("", "_Salary"))

    # Before merging, drop ClientID so it doesn't create a duplicate
    if "ClientID" in phone_calls.columns:
        phone_calls = phone_calls.drop(columns=["ClientID"])

    # Merge by Telephone
    if "Telephone" in phone_calls.columns and "Telephone" in client_data.columns:
        client_data = client_data.merge(
            phone_calls,
            on="Telephone",
            how="outer",
            suffixes=("", "_Phone"))

    # Export to Excel
    client_data.to_excel("/Users/sa17/Library/Mobile Documents/com~apple~CloudDocs/Brag Folder/projects/Womens-Mentoring-Network/data/clients_data.xlsx", index=False)
    return client_data


def main():
    # Extract
    intake, demographics, salaries, phone_calls, meetings = extract_excel()

    # Transform
    intake, demographics, salaries, phone_calls, meetings = transform_data(
    intake, demographics, salaries, phone_calls, meetings)

    # Load
    merged_data = merge_export(intake, demographics, salaries, phone_calls, meetings)
    print("Merging complete! File saved as clients_data.xlsx")


if __name__ == "__main__":
    main()

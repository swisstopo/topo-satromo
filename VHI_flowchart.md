```mermaid
graph TD
    A[Start VHI Processing] --> AA[Set Parameters]
    AA --> AB[Initialize Masks]
    AB --> AC[Set Processing Switches]
    AC --> AD[Define AOI]
    AD --> AE[Set Time Parameters]
    AE --> AF[Load S2 SR Data]
    AF --> B[Load NDVI Data]
    B --> C[Calculate VCI]
    AF --> D[Load LST Data]
    D --> E[Calculate TCI]
    C --> F[Combine VCI and TCI]
    E --> F
    F --> G[Calculate VHI]
    G --> H[Apply Vegetation/Forest Mask]
    H --> I[Export VHI]
    I --> J[End VHI Processing]

    subgraph NDVI Processing
    B --> B1[Load Reference NDVI]
    B1 --> B1a[Load NDVI Stats for DOY]
    B1a --> B1b[Adjust for Offset and Scale]
    B --> B2[Load Current NDVI]
    B2 --> B3[Apply Cloud/Snow/Shadow Mask]
    B3 --> B3a[Calculate NDSI]
    B3a --> B3b[Apply NDSI Mask]
    B3b --> B3c[Apply Terrain Shadow Mask]
    B3c --> B3d[Apply Cloud/Cloud Shadow Mask]
    B3d --> B4[Create NDVI Mosaic]
    B4 --> B4a[Sort by Time]
    B4a --> B4b[Create Latest Pixel Mosaic]
    B4b --> B4c[Calculate NDVI]
    B1b --> C
    B4c --> C
    end

    subgraph LST Processing
    D --> D1[Load Reference LST]
    D1 --> D1a[Load LST Stats for DOY]
    D1a --> D1b[Adjust for Scale]
    D --> D2[Load Current LST]
    D2 --> D3[Create LST Mosaic]
    D3 --> D3a[Filter LST Collection]
    D3a --> D3b[Sort by Time]
    D3b --> D3c[Create Latest Pixel Mosaic]
    D3c --> D3d[Select LST Band]
    D3d --> D3e[Adjust for Scale]
    D1b --> E
    D3e --> E
    end

    subgraph Data Checks
    K[Check LST Coverage]
    K --> K1{LST Data Available?}
    K1 -->|No| K2[Process LST from Raw Data]
    K2 --> K2a[Generate MSG LST Mosaic]
    K1 -->|Yes| L
    L[Check Existing VHI Asset]
    L --> L1{VHI Asset Exists?}
    L1 -->|Yes| J
    L1 -->|No| M
    M[Check Empty Asset List]
    M --> M1{In Empty Asset List?}
    M1 -->|Yes| J
    M1 -->|No| N
    N[Check S2 SR Data Availability]
    N --> N1{S2 SR Data Available?}
    N1 -->|No| N2[Mark as Empty Asset]
    N1 -->|Yes| AA
    end

    subgraph Parameter Setting
    AA --> AA1[Set AOI]
    AA --> AA2[Set Date Range]
    AA --> AA3[Set Alpha Value]
    AA --> AA4[Set CI Method]
    end

    subgraph Masking
    AB --> AB1[Load Vegetation Mask]
    AB --> AB2[Load Forest Mask]
    end

    subgraph Switches
    AC --> AC1[Set Export Options]
    AC --> AC2[Set Percentile Usage]
    end

    subgraph VCI Calculation
    C --> C1{Using Percentiles?}
    C1 -->|Yes| C2[Use 5th and 95th Percentiles]
    C1 -->|No| C3[Use Min and Max]
    C2 --> C4[Calculate VCI]
    C3 --> C4
    end

    subgraph TCI Calculation
    E --> E1{Using Percentiles?}
    E1 -->|Yes| E2[Use 5th and 95th Percentiles]
    E1 -->|No| E3[Use Min and Max]
    E2 --> E4[Calculate TCI]
    E3 --> E4
    end

    subgraph VHI Calculation
    G --> G1[Combine VCI and TCI]
    G1 --> G2[Apply Alpha Weight]
    end

    subgraph Export
    I --> I1[Export to GEE Asset]
    I --> I2[Export to Google Drive]
    end

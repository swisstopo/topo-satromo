# SATROMO Processing chain

A brief description of the project.

## Disclaimer

**Note: This project is currently in the proof of concept phase and is not intended for operational use.**

## Introduction and Project Description

This project aims to define automated spatial satellite products, indices, and analysis-ready datasets, as well as establish a geospatial data processing pipeline with traceable algorithms in the field of drought monitoring.
The aim is to have a serverless processing chain to derive and publish satrellite sensore derived products for drought monitoring. 
There are two enviroments
- DEV which is  a local machine with python installes
- PROD which is GitHub ACtion based .

## Installation

To install and set up the project, follow the instructions below:

1. Clone the repository to your local machine:
```
git clone https://github.com/swisstopo/topo-satromo.git
```
2. Install the project dependencies using pip:
```
pip install -r requirements.txt
```
3.Make sure you have Python 3.x and pip installed on your system.

[Optional] If you are using a virtual environment, activate it:
```
source <virtual-environment>/bin/activate
```
This step is recommended to isolate the project dependencies from your system-wide Python installation.

4. You are now ready to use the project!

For the use on DEV
- PROCESSOR: You need to create a folder "secrets" containing the GEE json according [Google Service Account](https://developers.google.com/earth-engine/guides/service_account) and a private key for the [service account](https://developers.google.com/earth-engine/guides/service_account#create-a-private-key-for-the-service-account)

For the use on PROD
You need in addition to trun the processor as well
- PUBLISHER: a rclone set up which transfers your data out of GDRIVE to you prefered location

## Usage

Describe how to use your Python code or library. Provide code examples, API documentation, or usage instructions to help users understand how to interact with your code effectively. Include any relevant screenshots or GIFs to demonstrate your project in action.

## Features

Highlight the key features of your project. Explain what makes it unique or different from existing solutions. You can provide a bullet-point list or describe each feature in detail.

## Contributing

If you want to encourage collaboration and contributions from others, provide guidelines on how to contribute to your project. Include instructions on how to set up the development environment, coding standards, and the process for submitting pull requests.

## License

Specify the license under which your project is released. It's important to make your licensing terms clear to potential users and contributors. Provide a license badge and include a section in your README.md file that outlines the license details.

## Credits and Acknowledgments

If your project builds upon or uses other open-source projects or libraries, acknowledge and give credit to those projects. Provide links to their repositories or relevant documentation.

## Contact Information

Include your contact information or ways for users to reach out to you. This can be in the form of an email address, social media handles, or a link to your personal website.

## Additional Sections

Depending on your project's complexity, you might want to include additional sections such as "Roadmap" (future plans and enhancements), "FAQs," or "Troubleshooting" (common issues and solutions).




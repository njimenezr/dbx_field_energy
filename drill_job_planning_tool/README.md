# Drilling Well Planner 

To run this bundle and create the assets for the Drilling Well Planner follow these instructions:

1. Download the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) on your local machine

2. Set up the Databricks CLI by creating a [PAT token](https://docs.databricks.com/en/dev-tools/auth/pat.html) and running `databricks configure` to set up your CLI 

3. With your editor of choice, open `databricks_template_schema.json` in the root of this folder and update the `properties:` section to match the type of compute and locations you'd like to use. After that, edit the targets section to match your enviornment
    - Please note: at this time the agent does not work with a Llama model. Make sure to specify a gpt model in the workspace you are deploying to. Make sure to set the appropriate permissions for the agent to use the model or you will get errors

4. Create a local folder for the bundle resources. From this folder, run the following terminal command to create the bundle:
   ```bash
   databricks bundle init <path-to-repo-folder>
   ```

5. Deploy and run the bundle by running the following terminal commands in order:
   ```bash
   databricks bundle validate -t <your-target-name>
   databricks bundle deploy -t <your-target-name>
   databricks bundle run -t <your-target-name> field-energy-drilling_well_job_pipeline_setup
   ```

6. Clean-up the deployed resources by running the following terminal command:
   ```bash
   databricks bundle destroy -t <your-target-name>
   ```

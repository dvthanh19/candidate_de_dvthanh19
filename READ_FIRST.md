# READ FIRST - Repository Access Guide

## Before You Start
⚠️ **CRITICAL**: Your access link opens only ONCE. Read this guide completely before clicking the link.

## Step 1: Get Your Repository Access
1. **PREPARE FIRST**: Open a text editor or notepad before clicking the link
2. Click your secure access link from the email
3. **COPY THE TOKEN**: You'll see a page with your access token
4. **SAVE TOKEN SECURELY**: Copy and save the token string (looks like: `glpat-xxxxxxxxxxxxxxxxxxxx`)
5. **KEEP SAFE**: This token is valid until your assessment deadline - store it securely
6. Do NOT close the page until you've copied and saved the token

## Step 2: Clone Repository
```bash
# Replace [YOUR_TOKEN] with the token provided by your administrator
git clone https://project_[ASSIGNED_PROJ_ID]_bot:[YOUR_TOKEN]@gitlab.com/doctorworld/data-analysis/assignments/[CANDIDATE_REPO_NAME].git
cd [CANDIDATE_REPO_NAME]
```

**💡 Token Reuse**: You can use the same token throughout your assessment for:
- Cloning on different machines
- Re-cloning if needed
- Pushing changes multiple times
- The token remains valid until your deadline

## Step 2.1: Configure Push Access (CRITICAL)
⚠️ **IMPORTANT**: After cloning, you must update the remote URL to enable push operations:

```bash
# Replace [YOUR_TOKEN] with your actual token
git remote set-url origin https://project_[ASSIGNED_PROJ_ID]_bot:[YOUR_TOKEN]@gitlab.com/doctorworld/data-analysis/assignments/[CANDIDATE_REPO_NAME].git

# Verify the remote URL includes your token
git remote -v
```

**💡 Why this step is needed**: Git clone downloads the code but doesn't store your token for future push operations. This step ensures you can push your work.

## Step 3: How to Submit Your Work

When you're ready to submit your completed assessment:

1. **Make sure you're in your repository directory:**
   ```bash
   # You should already be in your cloned repository
   pwd  # Should show your repository path
   ```

2. **Push your final work:**
   ```bash
   git add .
   git commit -m "Final submission: Complete ETL pipeline and analysis"
   git push origin main
   ```

3. **Email notification to HR** confirming your submission is complete

## What's Next?
After completing the setup above:

1. **Read the main README.md** for detailed assessment requirements
2. **Review data/erd_chart.png** for database schema
3. **Follow the complete instructions** in the repository README
4. **When finished, follow Step 3 above to submit your work**

## Troubleshooting
### Git Problems
- **Access denied**: Check if you're using the correct token from Step 1
- **Authentication failed**: Verify you're using the correct format `project_[ASSIGNED_PROJ_ID]_bot:[YOUR_TOKEN]`
- **Can't clone**: Make sure you replaced [YOUR_TOKEN] with your actual token
- **Push fails with "You are not allowed to upload code"**: 
  - Did you complete Step 2.1?
  - Check if token has expired
  - Verify your token has Developer role permissions
- **Invalid token format**: Token should look like `glpat-xxxxxxxxxxxxxxxxxxxx`

### Common Issues
- **Secure link expired**: Contact tien.pnt@wearecare.sg immediately for a new link
- **Lost the token**: If you forgot to save it, contact HR for assistance
- **Can't access Airflow**: Wait 5-10 minutes for full startup
- **Database connection issues**: Check PostgreSQL container is running
- **Token expired during assessment**: Contact tien.pnt@wearecare.sg
- **Need help**: Reply to the original email


Good luck! 🚀
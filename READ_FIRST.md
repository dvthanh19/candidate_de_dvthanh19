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
# Replace YOUR_TOKEN with the token you saved in Step 1
git clone https://gitlab-ci-token:YOUR_TOKEN@gitlab.com/wearecare/de-homework-assignment.git
cd de-homework-assignment
```

**💡 Token Reuse**: You can use the same token throughout your assessment for:
- Cloning on different machines
- Re-cloning if needed
- Pushing changes multiple times
- The token remains valid until your deadline

**Example with token:**
```bash
git clone https://gitlab-ci-token:glpat-xxxxxxxxxxxxxxxxxxxx@gitlab.com/wearecare/de-homework-assignment.git
```

## Step 3: Create Your Branch
```bash
# Replace "your-name" with your actual name (lowercase, hyphens for spaces)
# Example: candidate/john-smith
git checkout -b candidate/your-name
```

## What's Next?
After completing the setup above:

1. **Read the main README.md** for detailed assessment requirements
2. **Review data/erd_chart.png** for database schema
3. **Follow the complete instructions** in the repository README

## Troubleshooting
### Git Problems
- **Access denied**: Check if you're using the correct token from Step 1
- **Authentication failed**: Verify you included `gitlab-ci-token:` before your token
- **Can't clone**: Make sure you replaced YOUR_TOKEN with your actual token
- **Push fails**: Check if token has expired or if you're on the wrong branch
- Verify you're on your branch: `git branch`
- **Invalid token format**: Token should look like `glpat-xxxxxxxxxxxxxxxxxxxx`

### Common Issues
- **Secure link expired**: Contact tien.pnt@wearecare.sg immediately for a new link
- **Lost the token**: If you forgot to save it, contact HR for assistance
- **Can't access Airflow**: Wait 5-10 minutes for full startup
- **Database connection issues**: Check PostgreSQL container is running
- **Token expired during assessment**: Contact tien.pnt@wearecare.sg
- **Need help**: Reply to the original email


Good luck! 🚀
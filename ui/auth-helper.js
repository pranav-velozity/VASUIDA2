// auth-helper.js - Clerk Authentication for VelOzity
// =====================================================
// Add this file to /ui/public/ and load it BEFORE other scripts

(function() {
  'use strict';

  class VelOzityAuth {
    constructor() {
      this.clerk = null;
      this.user = null;
      this.session = null;
      this.isReady = false;
    }

    // Initialize Clerk
    async init(publishableKey) {
      try {
        // Wait for Clerk to load
        await this.waitForClerk();
        
        this.clerk = window.Clerk;
        await this.clerk.load({
          publishableKey: publishableKey
        });

        // Set up auth state
        if (this.clerk.user) {
          this.user = this.clerk.user;
          this.session = this.clerk.session;
          this.onSignedIn();
        } else {
          this.onSignedOut();
        }

        // Listen for changes
        this.clerk.addListener(({ user, session }) => {
          this.user = user;
          this.session = session;
          
          if (user && session) {
            this.onSignedIn();
          } else {
            this.onSignedOut();
          }
        });

        this.isReady = true;
      } catch (error) {
        console.error('[Auth] Initialization failed:', error);
      }
    }

    // Wait for Clerk script to load
    waitForClerk() {
      return new Promise((resolve) => {
        if (window.Clerk) return resolve();
        
        const check = setInterval(() => {
          if (window.Clerk) {
            clearInterval(check);
            resolve();
          }
        }, 100);
        
        // Timeout after 10 seconds
        setTimeout(() => {
          clearInterval(check);
          console.error('[Auth] Clerk failed to load');
          resolve();
        }, 10000);
      });
    }

    // Get authentication token
    async getToken() {
      if (!this.session) return null;
      try {
        return await this.session.getToken();
      } catch (error) {
        console.error('[Auth] Failed to get token:', error);
        return null;
      }
    }

    // Get user's role
    getUserRole() {
      if (!this.user) return null;
      const membership = this.user.organizationMemberships?.[0];
      return membership?.role;
    }

    // Get simplified role name
    getRoleName() {
      const role = this.getUserRole();
      const roleMap = {
        'org:admin_auth': 'admin',
        'org:supplier_auth': 'supplier',
        'org:client_auth': 'client',
        'org:api_auth': 'api'
      };
      return roleMap[role] || 'user';
    }

    // Check if user has specific role
    hasRole(roles) {
      const roleName = this.getRoleName();
      return roles.includes(roleName);
    }

    // Get user's organization
    getUserOrg() {
      if (!this.user) return null;
      return this.user.organizationMemberships?.[0]?.organization;
    }

    // Sign in
    async signIn() {
      if (!this.clerk) return;
      await this.clerk.openSignIn();
    }

    // Sign out
    async signOut() {
      if (!this.clerk) return;
      await this.clerk.signOut();
    }

    // Callback when user signs in
    onSignedIn() {
      console.log('[Auth] User signed in:', this.user?.firstName);
      
      // Show app, hide login
      const loginContainer = document.getElementById('auth-login-screen');
      const appContainer = document.getElementById('app-content');
      
      if (loginContainer) loginContainer.style.display = 'none';
      if (appContainer) appContainer.style.display = 'block';
      
      // Update UI
      this.updateUserUI();
      this.applyRoleBasedUI();
    }

    // Callback when user signs out
    onSignedOut() {
      console.log('[Auth] User signed out');
      
      // Hide app, show login
      const loginContainer = document.getElementById('auth-login-screen');
      const appContainer = document.getElementById('app-content');
      
      if (loginContainer) loginContainer.style.display = 'flex';
      if (appContainer) appContainer.style.display = 'none';
    }

    // Update UI with user info
    updateUserUI() {
      // User name
      const userNameEl = document.getElementById('user-name-display');
      if (userNameEl && this.user) {
        userNameEl.textContent = this.user.fullName || this.user.firstName || 'User';
      }

      // User role badge
      const userRoleEl = document.getElementById('user-role-display');
      if (userRoleEl) {
        const roleNames = {
          'admin': 'Admin',
          'supplier': 'Supplier',
          'client': 'Client',
          'api': 'API'
        };
        userRoleEl.textContent = roleNames[this.getRoleName()] || 'User';
      }

      // Organization
      const userOrgEl = document.getElementById('user-org-display');
      if (userOrgEl) {
        const org = this.getUserOrg();
        userOrgEl.textContent = org?.name || '';
      }
    }

    // Apply role-based UI visibility
    applyRoleBasedUI() {
      const roleName = this.getRoleName();
      
      // Hide features based on role
      document.querySelectorAll('[data-auth-role]').forEach(el => {
        const allowedRoles = el.getAttribute('data-auth-role').split(',').map(r => r.trim());
        
        if (roleName === 'admin' || allowedRoles.includes(roleName)) {
          el.style.display = '';
        } else {
          el.style.display = 'none';
        }
      });
    }

    // Make authenticated API call (use this instead of direct fetch)
    async authenticatedFetch(url, options = {}) {
      const token = await this.getToken();
      
      if (!token) {
        throw new Error('Not authenticated');
      }

      const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
        ...options.headers
      };

      const response = await fetch(url, {
        ...options,
        headers
      });

      // Handle 401 (token expired)
      if (response.status === 401) {
        console.error('[Auth] Token expired, please sign in again');
        await this.signOut();
        throw new Error('Session expired');
      }

      return response;
    }
  }

  // Create global auth instance
  window.velOzityAuth = new VelOzityAuth();

  // Auto-initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAuth);
  } else {
    initAuth();
  }

  async function initAuth() {
    // Get publishable key from meta tag or config
    const publishableKey = document.querySelector('meta[name="clerk-publishable-key"]')?.content;
    
    if (!publishableKey) {
      console.error('[Auth] Missing Clerk publishable key');
      return;
    }

    await window.velOzityAuth.init(publishableKey);
  }

})();

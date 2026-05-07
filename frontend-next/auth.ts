/**
 * next-auth v5 config – Credentials provider talking to FastAPI /auth/login.
 * Import { auth, signIn, signOut } from "@/auth" in server components/actions.
 */
import NextAuth from "next-auth";
import Credentials from "next-auth/providers/credentials";
import type { User } from "next-auth";

function backendBaseUrl(): string {
  const u =
    (process.env.AUTH_BACKEND_URL || process.env.NEXT_PUBLIC_API_URL || "").replace(/\/$/, "");
  if (!u && process.env.NODE_ENV === "production") {
    console.error(
      "[auth] Set NEXT_PUBLIC_API_URL (or AUTH_BACKEND_URL) to your FastAPI origin, e.g. https://your-api.vercel.app"
    );
  }
  return u || "http://localhost:8000";
}

export const { auth, handlers, signIn, signOut } = NextAuth({
  trustHost: true,
  providers: [
    Credentials({
      credentials: {
        email:    { label: "Email",    type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials): Promise<User | null> {
        const { email, password } = credentials as { email: string; password: string };
        try {
          const apiBase = backendBaseUrl();
          const res = await fetch(`${apiBase}/auth/login`, {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body:    JSON.stringify({ email, password }),
          });
          if (!res.ok) return null;
          const data = await res.json();
          return {
            id:             String(data.user.id),
            email:          data.user.email,
            name:           data.user.full_name,
            accessToken:    data.access_token,
            isAdmin:        data.user.is_admin,
            allowedPlants:  data.user.allowed_plants,
          } as User;
        } catch (e) {
          console.error("[auth] /auth/login fetch failed:", e);
          return null;
        }
      },
    }),
  ],
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        token.accessToken   = (user as never as Record<string, unknown>).accessToken as string;
        token.isAdmin       = (user as never as Record<string, unknown>).isAdmin as boolean;
        token.allowedPlants = (user as never as Record<string, unknown>).allowedPlants as string[];
      }
      return token;
    },
    async session({ session, token }) {
      (session as never as Record<string, unknown>).accessToken   = token.accessToken;
      (session.user as never as Record<string, unknown>).isAdmin       = token.isAdmin;
      (session.user as never as Record<string, unknown>).allowedPlants = token.allowedPlants;
      return session;
    },
  },
  pages: {
    signIn: "/login",
    error:  "/login",
  },
  session: { strategy: "jwt", maxAge: 8 * 60 * 60 },
  secret: process.env.AUTH_SECRET || process.env.NEXTAUTH_SECRET,
});

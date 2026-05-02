/**
 * next-auth v5 config – Credentials provider talking to FastAPI /auth/login.
 * Import { auth, signIn, signOut } from "@/auth" in server components/actions.
 */
import NextAuth from "next-auth";
import Credentials from "next-auth/providers/credentials";
import type { User } from "next-auth";

export const { auth, handlers, signIn, signOut } = NextAuth({
  providers: [
    Credentials({
      credentials: {
        email:    { label: "Email",    type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials): Promise<User | null> {
        const { email, password } = credentials as { email: string; password: string };
        try {
          const apiBase =
            process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
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
        } catch {
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
  secret: process.env.NEXTAUTH_SECRET,
});

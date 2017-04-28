// Discordgo - Discord bindings for Go
// Available at https://github.com/bwmarrin/discordgo

// Copyright 2015-2016 Bruce Marriner <bruce@sqls.net>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains functions for interacting with the Discord REST/JSON API
// at the lowest level.

package discordgo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg" // For JPEG decoding
	_ "image/png"  // For PNG decoding
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrJSONUnmarshal is returned for JSON Unmarshall errors.
var ErrJSONUnmarshal = errors.New("json unmarshal")

// Request is the same as RequestWithBucketID but the bucket id is the same as the urlStr
func (s *Session) Request(method, urlStr string, data interface{}) (response []byte, err error) {
	return s.RequestWithBucketID(method, urlStr, data, strings.SplitN(urlStr, "?", 2)[0])
}

// RequestWithBucketID makes a (GET/POST/...) Requests to Discord REST API with JSON data.
func (s *Session) RequestWithBucketID(method, urlStr string, data interface{}, bucketID string) (response []byte, err error) {
	var body []byte
	if data != nil {
		body, err = json.Marshal(data)
		if err != nil {
			return
		}
	}

	return s.request(method, urlStr, "application/json", body, bucketID, 0)
}

// request makes a (GET/POST/...) Requests to Discord REST API.
// Sequence is the sequence number, if it fails with a 502 it will
// retry with sequence+1 until it either succeeds or sequence >= session.MaxRestRetries
func (s *Session) request(method, urlStr, contentType string, b []byte, bucketID string, sequence int) (response []byte, err error) {
	if bucketID == "" {
		bucketID = strings.SplitN(urlStr, "?", 2)[0]
	}

	bucket := s.ratelimiter.LockBucket(bucketID)

	if s.Debug {
		log.Printf("API REQUEST %8s :: %s\n", method, urlStr)
		log.Printf("API REQUEST  PAYLOAD :: [%s]\n", string(b))
	}

	req, err := http.NewRequest(method, urlStr, bytes.NewBuffer(b))
	if err != nil {
		bucket.Release(nil)
		return
	}

	// Not used on initial login..
	// TODO: Verify if a login, otherwise complain about no-token
	if s.Token != "" {
		req.Header.Set("authorization", s.Token)
	}

	req.Header.Set("Content-Type", contentType)
	// TODO: Make a configurable static variable.
	req.Header.Set("User-Agent", fmt.Sprintf("DiscordBot (https://github.com/bwmarrin/discordgo, v%s)", VERSION))

	if s.Debug {
		for k, v := range req.Header {
			log.Printf("API REQUEST   HEADER :: [%s] = %+v\n", k, v)
		}
	}

	client := &http.Client{Timeout: (20 * time.Second)}

	resp, err := client.Do(req)
	if err != nil {
		bucket.Release(nil)
		return
	}
	defer func() {
		err2 := resp.Body.Close()
		if err2 != nil {
			log.Println("error closing resp body")
		}
	}()

	err = bucket.Release(resp.Header)
	if err != nil {
		return
	}

	response, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if s.Debug {

		log.Printf("API RESPONSE  STATUS :: %s\n", resp.Status)
		for k, v := range resp.Header {
			log.Printf("API RESPONSE  HEADER :: [%s] = %+v\n", k, v)
		}
		log.Printf("API RESPONSE    BODY :: [%s]\n\n\n", response)
	}

	switch resp.StatusCode {

	case http.StatusOK:
	case http.StatusCreated:
	case http.StatusNoContent:

		// TODO check for 401 response, invalidate token if we get one.

	case http.StatusBadGateway:
		// Retry sending request if possible
		if sequence < s.MaxRestRetries {

			s.log(LogInformational, "%s Failed (%s), Retrying...", urlStr, resp.Status)
			response, err = s.request(method, urlStr, contentType, b, bucketID, sequence+1)
		} else {
			err = fmt.Errorf("Exceeded Max retries HTTP %s, %s", resp.Status, response)
		}

	case 429: // TOO MANY REQUESTS - Rate limiting
		rl := TooManyRequests{}
		err = json.Unmarshal(response, &rl)
		if err != nil {
			s.log(LogError, "rate limit unmarshal error, %s", err)
			return
		}
		s.log(LogInformational, "Rate Limiting %s, retry in %d", urlStr, rl.RetryAfter)
		s.handleEvent(rateLimitEventType, RateLimit{TooManyRequests: &rl, URL: urlStr})

		time.Sleep(rl.RetryAfter * time.Millisecond)
		// we can make the above smarter
		// this method can cause longer delays than required

		response, err = s.request(method, urlStr, contentType, b, bucketID, sequence)

	default: // Error condition
		err = newRestError(req, resp, response)
	}

	return
}

func unmarshal(data []byte, v interface{}) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return ErrJSONUnmarshal
	}

	return nil
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Sessions
// ------------------------------------------------------------------------------------------------

// Login asks the Discord server for an authentication token.
func (s *Session) Login(email, password string) (err error) {

	data := struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}{email, password}

	response, err := s.RequestWithBucketID("POST", EndpointLogin, data, EndpointLogin)
	if err != nil {
		return
	}

	temp := struct {
		Token string `json:"token"`
	}{}

	err = unmarshal(response, &temp)
	if err != nil {
		return
	}

	s.Token = temp.Token
	return
}

// Register sends a Register request to Discord, and returns the authentication token
// Note that this account is temporary and should be verified for future use.
// Another option is to save the authentication token external, but this isn't recommended.
func (s *Session) Register(username string) (token string, err error) {

	data := struct {
		Username string `json:"username"`
	}{username}

	response, err := s.RequestWithBucketID("POST", EndpointRegister, data, EndpointRegister)
	if err != nil {
		return
	}

	temp := struct {
		Token string `json:"token"`
	}{}

	err = unmarshal(response, &temp)
	if err != nil {
		return
	}

	token = temp.Token
	return
}

// Logout sends a logout request to Discord.
// This does not seem to actually invalidate the token.  So you can still
// make API calls even after a Logout.  So, it seems almost pointless to
// even use.
func (s *Session) Logout() (err error) {

	//  _, err = s.Request("POST", LOGOUT, fmt.Sprintf(`{"token": "%s"}`, s.Token))

	if s.Token == "" {
		return
	}

	data := struct {
		Token string `json:"token"`
	}{s.Token}

	_, err = s.RequestWithBucketID("POST", EndpointLogout, data, EndpointLogout)
	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Users
// ------------------------------------------------------------------------------------------------

// User returns the user details of the given userID
// userID    : A user ID or "@me" which is a shortcut of current user ID
func (s *Session) User(userID string) (st *User, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointUser(userID), nil, EndpointUsers)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserAvatar returns an image.Image of a users Avatar.
// userID    : A user ID or "@me" which is a shortcut of current user ID
func (s *Session) UserAvatar(userID string) (img image.Image, err error) {
	u, err := s.User(userID)
	if err != nil {
		return
	}

	body, err := s.RequestWithBucketID("GET", EndpointUserAvatar(userID, u.Avatar), nil, EndpointUserAvatar("", ""))
	if err != nil {
		return
	}

	img, _, err = image.Decode(bytes.NewReader(body))
	return
}

// UserUpdate updates a users settings.
func (s *Session) UserUpdate(email, password, username, avatar, newPassword string) (st *User, err error) {

	// NOTE: Avatar must be either the hash/id of existing Avatar or
	// data:image/png;base64,BASE64_STRING_OF_NEW_AVATAR_PNG
	// to set a new avatar.
	// If left blank, avatar will be set to null/blank

	data := struct {
		Email       string `json:"email"`
		Password    string `json:"password"`
		Username    string `json:"username"`
		Avatar      string `json:"avatar,omitempty"`
		NewPassword string `json:"new_password,omitempty"`
	}{email, password, username, avatar, newPassword}

	body, err := s.RequestWithBucketID("PATCH", EndpointUser("@me"), data, EndpointUsers)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserSettings returns the settings for a given user
func (s *Session) UserSettings() (st *Settings, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointUserSettings("@me"), nil, EndpointUserSettings(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserUpdateStatus update the user status
// status   : The new status (Actual valid status are 'online','idle','dnd','invisible')
func (s *Session) UserUpdateStatus(status Status) (st *Settings, err error) {
	if status == StatusOffline {
		err = errors.New("You can't set your Status to offline")
		return
	}

	data := struct {
		Status Status `json:"status"`
	}{status}

	body, err := s.RequestWithBucketID("PATCH", EndpointUserSettings("@me"), data, EndpointUserSettings(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserChannels returns an array of Channel structures for all private
// channels.
func (s *Session) UserChannels() (st []*Channel, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointUserChannels("@me"), nil, EndpointUserChannels(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserChannelCreate creates a new User (Private) Channel with another User
// recipientID : A user ID for the user to which this channel is opened with.
func (s *Session) UserChannelCreate(recipientID string) (st *Channel, err error) {

	data := struct {
		RecipientID string `json:"recipient_id"`
	}{recipientID}

	body, err := s.RequestWithBucketID("POST", EndpointUserChannels("@me"), data, EndpointUserChannels(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserGuilds returns an array of UserGuild structures for all guilds.
func (s *Session) UserGuilds() (st []*UserGuild, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointUserGuilds("@me"), nil, EndpointUserGuilds(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserGuildSettingsEdit Edits the users notification settings for a guild
// guildID   : The ID of the guild to edit the settings on
// settings  : The settings to update
func (s *Session) UserGuildSettingsEdit(guildID string, settings *UserGuildSettingsEdit) (st *UserGuildSettings, err error) {

	body, err := s.RequestWithBucketID("PATCH", EndpointUserGuildSettings("@me", guildID), settings, EndpointUserGuildSettings("", guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// UserChannelPermissions returns the permission of a user in a channel.
// userID    : The ID of the user to calculate permissions for.
// channelID : The ID of the channel to calculate permission for.
//
// NOTE: This function is now deprecated and will be removed in the future.
// Please see the same function inside state.go
func (s *Session) UserChannelPermissions(userID, channelID string) (apermissions int, err error) {
	channel, err := s.State.Channel(channelID)
	if err != nil || channel == nil {
		channel, err = s.Channel(channelID)
		if err != nil {
			return
		}
	}

	guild, err := s.State.Guild(channel.GuildID)
	if err != nil || guild == nil {
		guild, err = s.Guild(channel.GuildID)
		if err != nil {
			return
		}
	}

	if userID == guild.OwnerID {
		apermissions = PermissionAll
		return
	}

	member, err := s.State.Member(guild.ID, userID)
	if err != nil || member == nil {
		member, err = s.GuildMember(guild.ID, userID)
		if err != nil {
			return
		}
	}

	for _, role := range guild.Roles {
		if role.ID == guild.ID {
			apermissions |= role.Permissions
			break
		}
	}

	for _, role := range guild.Roles {
		for _, roleID := range member.Roles {
			if role.ID == roleID {
				apermissions |= role.Permissions
				break
			}
		}
	}

	if apermissions&PermissionAdministrator > 0 {
		apermissions |= PermissionAll
	}

	// Member overwrites can override role overrides, so do two passes
	for _, overwrite := range channel.PermissionOverwrites {
		for _, roleID := range member.Roles {
			if overwrite.Type == "role" && roleID == overwrite.ID {
				apermissions &= ^overwrite.Deny
				apermissions |= overwrite.Allow
				break
			}
		}
	}

	for _, overwrite := range channel.PermissionOverwrites {
		if overwrite.Type == "member" && overwrite.ID == userID {
			apermissions &= ^overwrite.Deny
			apermissions |= overwrite.Allow
			break
		}
	}

	if apermissions&PermissionAdministrator > 0 {
		apermissions |= PermissionAllChannel
	}

	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Guilds
// ------------------------------------------------------------------------------------------------

// Guild returns a Guild structure of a specific Guild.
// guildID   : The ID of a Guild
func (s *Session) Guild(guildID string) (st *Guild, err error) {
	if s.StateEnabled {
		// Attempt to grab the guild from State first.
		st, err = s.State.Guild(guildID)
		if err == nil {
			return
		}
	}

	body, err := s.RequestWithBucketID("GET", EndpointGuild(guildID), nil, EndpointGuild(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildCreate creates a new Guild
// name      : A name for the Guild (2-100 characters)
func (s *Session) GuildCreate(name string) (st *Guild, err error) {

	data := struct {
		Name string `json:"name"`
	}{name}

	body, err := s.RequestWithBucketID("POST", EndpointGuilds, data, EndpointGuilds)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildEdit edits a new Guild
// guildID   : The ID of a Guild
// g 		 : A GuildParams struct with the values Name, Region and VerificationLevel defined.
func (s *Session) GuildEdit(guildID string, g GuildParams) (st *Guild, err error) {

	// Bounds checking for VerificationLevel, interval: [0, 3]
	if g.VerificationLevel != nil {
		val := *g.VerificationLevel
		if val < 0 || val > 3 {
			err = errors.New("VerificationLevel out of bounds, should be between 0 and 3")
			return
		}
	}

	//Bounds checking for regions
	if g.Region != "" {
		isValid := false
		regions, _ := s.VoiceRegions()
		for _, r := range regions {
			if g.Region == r.ID {
				isValid = true
			}
		}
		if !isValid {
			var valid []string
			for _, r := range regions {
				valid = append(valid, r.ID)
			}
			err = fmt.Errorf("Region not a valid region (%q)", valid)
			return
		}
	}

	data := struct {
		Name              string             `json:"name,omitempty"`
		Region            string             `json:"region,omitempty"`
		VerificationLevel *VerificationLevel `json:"verification_level,omitempty"`
	}{g.Name, g.Region, g.VerificationLevel}

	body, err := s.RequestWithBucketID("PATCH", EndpointGuild(guildID), data, EndpointGuild(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildDelete deletes a Guild.
// guildID   : The ID of a Guild
func (s *Session) GuildDelete(guildID string) (st *Guild, err error) {

	body, err := s.RequestWithBucketID("DELETE", EndpointGuild(guildID), nil, EndpointGuild(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildLeave leaves a Guild.
// guildID   : The ID of a Guild
func (s *Session) GuildLeave(guildID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointUserGuild("@me", guildID), nil, EndpointUserGuild("", guildID))
	return
}

// GuildBans returns an array of User structures for all bans of a
// given guild.
// guildID   : The ID of a Guild.
func (s *Session) GuildBans(guildID string) (st []*GuildBan, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointGuildBans(guildID), nil, EndpointGuildBans(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildBanCreate bans the given user from the given guild.
// guildID   : The ID of a Guild.
// userID    : The ID of a User
// days      : The number of days of previous comments to delete.
func (s *Session) GuildBanCreate(guildID, userID string, days int) (err error) {

	uri := EndpointGuildBan(guildID, userID)

	if days > 0 {
		uri = fmt.Sprintf("%s?delete-message-days=%d", uri, days)
	}

	_, err = s.RequestWithBucketID("PUT", uri, nil, EndpointGuildBan(guildID, ""))
	return
}

// GuildBanDelete removes the given user from the guild bans
// guildID   : The ID of a Guild.
// userID    : The ID of a User
func (s *Session) GuildBanDelete(guildID, userID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointGuildBan(guildID, userID), nil, EndpointGuildBan(guildID, ""))
	return
}

// GuildMembers returns a list of members for a guild.
//  guildID  : The ID of a Guild.
//  after    : The id of the member to return members after
//  limit    : max number of members to return (max 1000)
func (s *Session) GuildMembers(guildID string, after string, limit int) (st []*Member, err error) {

	uri := EndpointGuildMembers(guildID)

	v := url.Values{}

	if after != "" {
		v.Set("after", after)
	}

	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}

	if len(v) > 0 {
		uri = fmt.Sprintf("%s?%s", uri, v.Encode())
	}

	body, err := s.RequestWithBucketID("GET", uri, nil, EndpointGuildMembers(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildMember returns a member of a guild.
//  guildID   : The ID of a Guild.
//  userID    : The ID of a User
func (s *Session) GuildMember(guildID, userID string) (st *Member, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointGuildMember(guildID, userID), nil, EndpointGuildMember(guildID, ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildMemberDelete removes the given user from the given guild.
// guildID   : The ID of a Guild.
// userID    : The ID of a User
func (s *Session) GuildMemberDelete(guildID, userID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointGuildMember(guildID, userID), nil, EndpointGuildMember(guildID, ""))
	return
}

// GuildMemberEdit edits the roles of a member.
// guildID  : The ID of a Guild.
// userID   : The ID of a User.
// roles    : A list of role ID's to set on the member.
func (s *Session) GuildMemberEdit(guildID, userID string, roles []string) (err error) {

	data := struct {
		Roles []string `json:"roles"`
	}{roles}

	_, err = s.RequestWithBucketID("PATCH", EndpointGuildMember(guildID, userID), data, EndpointGuildMember(guildID, ""))
	if err != nil {
		return
	}

	return
}

// GuildMemberMove moves a guild member from one voice channel to another/none
//  guildID   : The ID of a Guild.
//  userID    : The ID of a User.
//  channelID : The ID of a channel to move user to, or null?
// NOTE : I am not entirely set on the name of this function and it may change
// prior to the final 1.0.0 release of Discordgo
func (s *Session) GuildMemberMove(guildID, userID, channelID string) (err error) {

	data := struct {
		ChannelID string `json:"channel_id"`
	}{channelID}

	_, err = s.RequestWithBucketID("PATCH", EndpointGuildMember(guildID, userID), data, EndpointGuildMember(guildID, ""))
	if err != nil {
		return
	}

	return
}

// GuildMemberNickname updates the nickname of a guild member
// guildID   : The ID of a guild
// userID    : The ID of a user
func (s *Session) GuildMemberNickname(guildID, userID, nickname string) (err error) {

	data := struct {
		Nick string `json:"nick"`
	}{nickname}

	_, err = s.RequestWithBucketID("PATCH", EndpointGuildMember(guildID, userID), data, EndpointGuildMember(guildID, ""))
	return
}

// GuildMemberRoleAdd adds the specified role to a given member
//  guildID   : The ID of a Guild.
//  userID    : The ID of a User.
//  roleID 	  : The ID of a Role to be assigned to the user.
func (s *Session) GuildMemberRoleAdd(guildID, userID, roleID string) (err error) {

	_, err = s.RequestWithBucketID("PUT", EndpointGuildMemberRole(guildID, userID, roleID), nil, EndpointGuildMemberRole(guildID, userID, roleID))

	return
}

// GuildMemberRoleRemove removes the specified role to a given member
//  guildID   : The ID of a Guild.
//  userID    : The ID of a User.
//  roleID 	  : The ID of a Role to be removed from the user.
func (s *Session) GuildMemberRoleRemove(guildID, userID, roleID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointGuildMemberRole(guildID, userID, roleID), nil, EndpointGuildMemberRole(guildID, userID, roleID))

	return
}

// GuildChannels returns an array of Channel structures for all channels of a
// given guild.
// guildID   : The ID of a Guild.
func (s *Session) GuildChannels(guildID string) (st []*Channel, err error) {

	body, err := s.request("GET", EndpointGuildChannels(guildID), "", nil, EndpointGuildChannels(guildID), 0)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildChannelCreate creates a new channel in the given guild
// guildID   : The ID of a Guild.
// name      : Name of the channel (2-100 chars length)
// ctype     : Tpye of the channel (voice or text)
func (s *Session) GuildChannelCreate(guildID, name, ctype string) (st *Channel, err error) {

	data := struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}{name, ctype}

	body, err := s.RequestWithBucketID("POST", EndpointGuildChannels(guildID), data, EndpointGuildChannels(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildChannelsReorder updates the order of channels in a guild
// guildID   : The ID of a Guild.
// channels  : Updated channels.
func (s *Session) GuildChannelsReorder(guildID string, channels []*Channel) (err error) {

	_, err = s.RequestWithBucketID("PATCH", EndpointGuildChannels(guildID), channels, EndpointGuildChannels(guildID))
	return
}

// GuildInvites returns an array of Invite structures for the given guild
// guildID   : The ID of a Guild.
func (s *Session) GuildInvites(guildID string) (st []*Invite, err error) {
	body, err := s.RequestWithBucketID("GET", EndpointGuildInvites(guildID), nil, EndpointGuildInivtes(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildRoles returns all roles for a given guild.
// guildID   : The ID of a Guild.
func (s *Session) GuildRoles(guildID string) (st []*Role, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointGuildRoles(guildID), nil, EndpointGuildRoles(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return // TODO return pointer
}

// GuildRoleCreate returns a new Guild Role.
// guildID: The ID of a Guild.
func (s *Session) GuildRoleCreate(guildID string) (st *Role, err error) {

	body, err := s.RequestWithBucketID("POST", EndpointGuildRoles(guildID), nil, EndpointGuildRoles(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildRoleEdit updates an existing Guild Role with new values
// guildID   : The ID of a Guild.
// roleID    : The ID of a Role.
// name      : The name of the Role.
// color     : The color of the role (decimal, not hex).
// hoist     : Whether to display the role's users separately.
// perm      : The permissions for the role.
// mention   : Whether this role is mentionable
func (s *Session) GuildRoleEdit(guildID, roleID, name string, color int, hoist bool, perm int, mention bool) (st *Role, err error) {

	// Prevent sending a color int that is too big.
	if color > 0xFFFFFF {
		err = fmt.Errorf("color value cannot be larger than 0xFFFFFF")
	}

	data := struct {
		Name        string `json:"name"`        // The role's name (overwrites existing)
		Color       int32    `json:"color"`       // The color the role should have (as a decimal, not hex)
		Hoist       bool   `json:"hoist"`       // Whether to display the role's users separately
		Permissions int32    `json:"permissions"` // The overall permissions number of the role (overwrites existing)
		Mentionable bool   `json:"mentionable"` // Whether this role is mentionable
	}{name, color, hoist, perm, mention}

	body, err := s.RequestWithBucketID("PATCH", EndpointGuildRole(guildID, roleID), data, EndpointGuildRole(guildID, ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildRoleReorder reoders guild roles
// guildID   : The ID of a Guild.
// roles     : A list of ordered roles.
func (s *Session) GuildRoleReorder(guildID string, roles []*Role) (st []*Role, err error) {

	body, err := s.RequestWithBucketID("PATCH", EndpointGuildRoles(guildID), roles, EndpointGuildRoles(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildRoleDelete deletes an existing role.
// guildID   : The ID of a Guild.
// roleID    : The ID of a Role.
func (s *Session) GuildRoleDelete(guildID, roleID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointGuildRole(guildID, roleID), nil, EndpointGuildRole(guildID, ""))

	return
}

// GuildPruneCount Returns the number of members that would be removed in a prune operation.
// Requires 'KICK_MEMBER' permission.
// guildID	: The ID of a Guild.
// days		: The number of days to count prune for (1 or more).
func (s *Session) GuildPruneCount(guildID string, days uint32) (count uint32, err error) {
	count = 0

	if days <= 0 {
		err = errors.New("The number of days should be more than or equal to 1.")
		return
	}

	p := struct {
		Pruned uint32 `json:"pruned"`
	}{}

	uri := EndpointGuildPrune(guildID) + fmt.Sprintf("?days=%d", days)
	body, err := s.RequestWithBucketID("GET", uri, nil, EndpointGuildPrune(guildID))

	err = unmarshal(body, &p)
	if err != nil {
		return
	}

	count = p.Pruned

	return
}

// GuildPrune Begin as prune operation. Requires the 'KICK_MEMBERS' permission.
// Returns an object with one 'pruned' key indicating the number of members that were removed in the prune operation.
// guildID	: The ID of a Guild.
// days		: The number of days to count prune for (1 or more).
func (s *Session) GuildPrune(guildID string, days uint32) (count uint32, err error) {

	count = 0

	if days <= 0 {
		err = errors.New("The number of days should be more than or equal to 1.")
		return
	}

	data := struct {
		days uint32
	}{days}

	p := struct {
		Pruned uint32 `json:"pruned"`
	}{}

	body, err := s.RequestWithBucketID("POST", EndpointGuildPrune(guildID), data, EndpointGuildPrune(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &p)
	if err != nil {
		return
	}

	count = p.Pruned

	return
}

// GuildIntegrations returns an array of Integrations for a guild.
// guildID   : The ID of a Guild.
func (s *Session) GuildIntegrations(guildID string) (st []*GuildIntegration, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointGuildIntegrations(guildID), nil, EndpointGuildIntegrations(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildIntegrationCreate creates a Guild Integration.
// guildID          : The ID of a Guild.
// integrationType  : The Integration type.
// integrationID    : The ID of an integration.
func (s *Session) GuildIntegrationCreate(guildID, integrationType, integrationID string) (err error) {

	data := struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}{integrationType, integrationID}

	_, err = s.RequestWithBucketID("POST", EndpointGuildIntegrations(guildID), data, EndpointGuildIntegrations(guildID))
	return
}

// GuildIntegrationEdit edits a Guild Integration.
// guildID              : The ID of a Guild.
// integrationType      : The Integration type.
// integrationID        : The ID of an integration.
// expireBehavior	      : The behavior when an integration subscription lapses (see the integration object documentation).
// expireGracePeriod    : Period (in seconds) where the integration will ignore lapsed subscriptions.
// enableEmoticons	    : Whether emoticons should be synced for this integration (twitch only currently).
func (s *Session) GuildIntegrationEdit(guildID, integrationID string, expireBehavior, expireGracePeriod int, enableEmoticons bool) (err error) {

	data := struct {
		ExpireBehavior    int32`json:"expire_behavior"`
		ExpireGracePeriod int32`json:"expire_grace_period"`
		EnableEmoticons   bool `json:"enable_emoticons"`
	}{expireBehavior, expireGracePeriod, enableEmoticons}

	_, err = s.RequestWithBucketID("PATCH", EndpointGuildIntegration(guildID, integrationID), data, EndpointGuildIntegration(guildID, ""))
	return
}

// GuildIntegrationDelete removes the given integration from the Guild.
// guildID          : The ID of a Guild.
// integrationID    : The ID of an integration.
func (s *Session) GuildIntegrationDelete(guildID, integrationID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointGuildIntegration(guildID, integrationID), nil, EndpointGuildIntegration(guildID, ""))
	return
}

// GuildIntegrationSync syncs an integration.
// guildID          : The ID of a Guild.
// integrationID    : The ID of an integration.
func (s *Session) GuildIntegrationSync(guildID, integrationID string) (err error) {

	_, err = s.RequestWithBucketID("POST", EndpointGuildIntegrationSync(guildID, integrationID), nil, EndpointGuildIntegration(guildID, ""))
	return
}

// GuildIcon returns an image.Image of a guild icon.
// guildID   : The ID of a Guild.
func (s *Session) GuildIcon(guildID string) (img image.Image, err error) {
	g, err := s.Guild(guildID)
	if err != nil {
		return
	}

	if g.Icon == "" {
		err = errors.New("Guild does not have an icon set.")
		return
	}

	body, err := s.RequestWithBucketID("GET", EndpointGuildIcon(guildID, g.Icon), nil, EndpointGuildIcon(guildID, ""))
	if err != nil {
		return
	}

	img, _, err = image.Decode(bytes.NewReader(body))
	return
}

// GuildSplash returns an image.Image of a guild splash image.
// guildID   : The ID of a Guild.
func (s *Session) GuildSplash(guildID string) (img image.Image, err error) {
	g, err := s.Guild(guildID)
	if err != nil {
		return
	}

	if g.Splash == "" {
		err = errors.New("Guild does not have a splash set.")
		return
	}

	body, err := s.RequestWithBucketID("GET", EndpointGuildSplash(guildID, g.Splash), nil, EndpointGuildSplash(guildID, ""))
	if err != nil {
		return
	}

	img, _, err = image.Decode(bytes.NewReader(body))
	return
}

// GuildEmbed returns the embed for a Guild.
// guildID   : The ID of a Guild.
func (s *Session) GuildEmbed(guildID string) (st *GuildEmbed, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointGuildEmbed(guildID), nil, EndpointGuildEmbed(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// GuildEmbedEdit returns the embed for a Guild.
// guildID   : The ID of a Guild.
func (s *Session) GuildEmbedEdit(guildID string, enabled bool, channelID string) (err error) {

	data := GuildEmbed{enabled, channelID}

	_, err = s.RequestWithBucketID("PATCH", EndpointGuildEmbed(guildID), data, EndpointGuildEmbed(guildID))
	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Channels
// ------------------------------------------------------------------------------------------------

// Channel returns a Channel strucutre of a specific Channel.
// channelID  : The ID of the Channel you want returned.
func (s *Session) Channel(channelID string) (st *Channel, err error) {
	body, err := s.RequestWithBucketID("GET", EndpointChannel(channelID), nil, EndpointChannel(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelEdit edits the given channel
// channelID  : The ID of a Channel
// name       : The new name to assign the channel.
func (s *Session) ChannelEdit(channelID, name string) (st *Channel, err error) {

	data := struct {
		Name string `json:"name"`
	}{name}

	body, err := s.RequestWithBucketID("PATCH", EndpointChannel(channelID), data, EndpointChannel(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelDelete deletes the given channel
// channelID  : The ID of a Channel
func (s *Session) ChannelDelete(channelID string) (st *Channel, err error) {

	body, err := s.RequestWithBucketID("DELETE", EndpointChannel(channelID), nil, EndpointChannel(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelTyping broadcasts to all members that authenticated user is typing in
// the given channel.
// channelID  : The ID of a Channel
func (s *Session) ChannelTyping(channelID string) (err error) {

	_, err = s.RequestWithBucketID("POST", EndpointChannelTyping(channelID), nil, EndpointChannelTyping(channelID))
	return
}

// ChannelMessages returns an array of Message structures for messages within
// a given channel.
// channelID : The ID of a Channel.
// limit     : The number messages that can be returned. (max 100)
// beforeID  : If provided all messages returned will be before given ID.
// afterID   : If provided all messages returned will be after given ID.
func (s *Session) ChannelMessages(channelID string, limit int, beforeID, afterID string) (st []*Message, err error) {

	uri := EndpointChannelMessages(channelID)

	v := url.Values{}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	if afterID != "" {
		v.Set("after", afterID)
	}
	if beforeID != "" {
		v.Set("before", beforeID)
	}
	if len(v) > 0 {
		uri = fmt.Sprintf("%s?%s", uri, v.Encode())
	}

	body, err := s.RequestWithBucketID("GET", uri, nil, EndpointChannelMessages(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelMessage gets a single message by ID from a given channel.
// channeld  : The ID of a Channel
// messageID : the ID of a Message
func (s *Session) ChannelMessage(channelID, messageID string) (st *Message, err error) {

	response, err := s.RequestWithBucketID("GET", EndpointChannelMessage(channelID, messageID), nil, EndpointChannelMessage(channelID, ""))
	if err != nil {
		return
	}

	err = unmarshal(response, &st)
	return
}

// ChannelMessageAck acknowledges and marks the given message as read
// channeld  : The ID of a Channel
// messageID : the ID of a Message
// lastToken : token returned by last ack
func (s *Session) ChannelMessageAck(channelID, messageID, lastToken string) (st *Ack, err error) {

	body, err := s.RequestWithBucketID("POST", EndpointChannelMessageAck(channelID, messageID), &Ack{Token: lastToken}, EndpointChannelMessageAck(channelID, ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// channelMessageSend sends a message to the given channel.
// channelID : The ID of a Channel.
// content   : The message to send.
// tts       : Whether to send the message with TTS.
func (s *Session) channelMessageSend(channelID, content string, tts bool) (st *Message, err error) {

	// TODO: nonce string ?
	data := struct {
		Content string `json:"content"`
		TTS     bool   `json:"tts"`
	}{content, tts}

	// Send the message to the given channel
	response, err := s.RequestWithBucketID("POST", EndpointChannelMessages(channelID), data, EndpointChannelMessages(channelID))
	if err != nil {
		return
	}

	err = unmarshal(response, &st)
	return
}

// ChannelMessageSend sends a message to the given channel.
// channelID : The ID of a Channel.
// content   : The message to send.
func (s *Session) ChannelMessageSend(channelID string, content string) (st *Message, err error) {

	return s.channelMessageSend(channelID, content, false)
}

// ChannelMessageSendTTS sends a message to the given channel with Text to Speech.
// channelID : The ID of a Channel.
// content   : The message to send.
func (s *Session) ChannelMessageSendTTS(channelID string, content string) (st *Message, err error) {

	return s.channelMessageSend(channelID, content, true)
}

// ChannelMessageSendEmbed sends a message to the given channel with embedded data (bot only).
// channelID : The ID of a Channel.
// embed     : The embed data to send.
func (s *Session) ChannelMessageSendEmbed(channelID string, embed *MessageEmbed) (st *Message, err error) {
	if embed != nil && embed.Type == "" {
		embed.Type = "rich"
	}

	data := struct {
		Embed *MessageEmbed `json:"embed"`
	}{embed}

	// Send the message to the given channel
	response, err := s.RequestWithBucketID("POST", EndpointChannelMessages(channelID), data, EndpointChannelMessages(channelID))
	if err != nil {
		return
	}

	err = unmarshal(response, &st)
	return
}

// ChannelMessageEdit edits an existing message, replacing it entirely with
// the given content.
// channeld  : The ID of a Channel
// messageID : the ID of a Message
func (s *Session) ChannelMessageEdit(channelID, messageID, content string) (st *Message, err error) {

	data := struct {
		Content string `json:"content"`
	}{content}

	response, err := s.RequestWithBucketID("PATCH", EndpointChannelMessage(channelID, messageID), data, EndpointChannelMessage(channelID, ""))
	if err != nil {
		return
	}

	err = unmarshal(response, &st)
	return
}

// ChannelMessageEditEmbed edits an existing message with embedded data (bot only).
// channelID : The ID of a Channel
// messageID : The ID of a Message
// embed     : The embed data to send
func (s *Session) ChannelMessageEditEmbed(channelID, messageID string, embed *MessageEmbed) (st *Message, err error) {
	if embed != nil && embed.Type == "" {
		embed.Type = "rich"
	}

	data := struct {
		Embed *MessageEmbed `json:"embed"`
	}{embed}

	response, err := s.RequestWithBucketID("PATCH", EndpointChannelMessage(channelID, messageID), data, EndpointChannelMessage(channelID, ""))
	if err != nil {
		return
	}

	err = unmarshal(response, &st)
	return
}

// ChannelMessageDelete deletes a message from the Channel.
func (s *Session) ChannelMessageDelete(channelID, messageID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointChannelMessage(channelID, messageID), nil, EndpointChannelMessage(channelID, ""))
	return
}

// ChannelMessagesBulkDelete bulk deletes the messages from the channel for the provided messageIDs.
// If only one messageID is in the slice call channelMessageDelete funciton.
// If the slice is empty do nothing.
// channelID : The ID of the channel for the messages to delete.
// messages  : The IDs of the messages to be deleted. A slice of string IDs. A maximum of 100 messages.
func (s *Session) ChannelMessagesBulkDelete(channelID string, messages []string) (err error) {

	if len(messages) == 0 {
		return
	}

	if len(messages) == 1 {
		err = s.ChannelMessageDelete(channelID, messages[0])
		return
	}

	if len(messages) > 100 {
		messages = messages[:100]
	}

	data := struct {
		Messages []string `json:"messages"`
	}{messages}

	_, err = s.RequestWithBucketID("POST", EndpointChannelMessagesBulkDelete(channelID), data, EndpointChannelMessagesBulkDelete(channelID))
	return
}

// ChannelMessagePin pins a message within a given channel.
// channelID: The ID of a channel.
// messageID: The ID of a message.
func (s *Session) ChannelMessagePin(channelID, messageID string) (err error) {

	_, err = s.RequestWithBucketID("PUT", EndpointChannelMessagePin(channelID, messageID), nil, EndpointChannelMessagePin(channelID, ""))
	return
}

// ChannelMessageUnpin unpins a message within a given channel.
// channelID: The ID of a channel.
// messageID: The ID of a message.
func (s *Session) ChannelMessageUnpin(channelID, messageID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointChannelMessagePin(channelID, messageID), nil, EndpointChannelMessagePin(channelID, ""))
	return
}

// ChannelMessagesPinned returns an array of Message structures for pinned messages
// within a given channel
// channelID : The ID of a Channel.
func (s *Session) ChannelMessagesPinned(channelID string) (st []*Message, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointChannelMessagesPins(channelID), nil, EndpointChannelMessagesPins(channelID))

	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelFileSend sends a file to the given channel.
// channelID : The ID of a Channel.
// name: The name of the file.
// io.Reader : A reader for the file contents.
func (s *Session) ChannelFileSend(channelID, name string, r io.Reader) (st *Message, err error) {
	return s.ChannelFileSendWithMessage(channelID, "", name, r)
}

// ChannelFileSendWithMessage sends a file to the given channel with an message.
// channelID : The ID of a Channel.
// content: Optional Message content.
// name: The name of the file.
// io.Reader : A reader for the file contents.
func (s *Session) ChannelFileSendWithMessage(channelID, content string, name string, r io.Reader) (st *Message, err error) {

	body := &bytes.Buffer{}
	bodywriter := multipart.NewWriter(body)

	if len(content) != 0 {
		if err := bodywriter.WriteField("content", content); err != nil {
			return nil, err
		}
	}

	writer, err := bodywriter.CreateFormFile("file", name)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(writer, r)
	if err != nil {
		return
	}

	err = bodywriter.Close()
	if err != nil {
		return
	}

	response, err := s.request("POST", EndpointChannelMessages(channelID), bodywriter.FormDataContentType(), body.Bytes(), EndpointChannelMessages(channelID), 0)
	if err != nil {
		return
	}

	err = unmarshal(response, &st)
	return
}

// ChannelInvites returns an array of Invite structures for the given channel
// channelID   : The ID of a Channel
func (s *Session) ChannelInvites(channelID string) (st []*Invite, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointChannelInvites(channelID), nil, EndpointChannelInvites(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelInviteCreate creates a new invite for the given channel.
// channelID   : The ID of a Channel
// i           : An Invite struct with the values MaxAge, MaxUses, Temporary,
//               and XkcdPass defined.
func (s *Session) ChannelInviteCreate(channelID string, i Invite) (st *Invite, err error) {

	data := struct {
		MaxAge    int32  `json:"max_age"`
		MaxUses   int32  `json:"max_uses"`
		Temporary bool   `json:"temporary"`
		XKCDPass  string `json:"xkcdpass"`
	}{i.MaxAge, i.MaxUses, i.Temporary, i.XkcdPass}

	body, err := s.RequestWithBucketID("POST", EndpointChannelInvites(channelID), data, EndpointChannelInvites(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ChannelPermissionSet creates a Permission Override for the given channel.
// NOTE: This func name may changed.  Using Set instead of Create because
// you can both create a new override or update an override with this function.
func (s *Session) ChannelPermissionSet(channelID, targetID, targetType string, allow, deny int) (err error) {

	data := struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Allow int32  `json:"allow"`
		Deny  int32  `json:"deny"`
	}{targetID, targetType, allow, deny}

	_, err = s.RequestWithBucketID("PUT", EndpointChannelPermission(channelID, targetID), data, EndpointChannelPermission(channelID, ""))
	return
}

// ChannelPermissionDelete deletes a specific permission override for the given channel.
// NOTE: Name of this func may change.
func (s *Session) ChannelPermissionDelete(channelID, targetID string) (err error) {

	_, err = s.RequestWithBucketID("DELETE", EndpointChannelPermission(channelID, targetID), nil, EndpointChannelPermission(channelID, ""))
	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Invites
// ------------------------------------------------------------------------------------------------

// Invite returns an Invite structure of the given invite
// inviteID : The invite code (or maybe xkcdpass?)
func (s *Session) Invite(inviteID string) (st *Invite, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointInvite(inviteID), nil, EndpointInvite(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// InviteDelete deletes an existing invite
// inviteID   : the code (or maybe xkcdpass?) of an invite
func (s *Session) InviteDelete(inviteID string) (st *Invite, err error) {

	body, err := s.RequestWithBucketID("DELETE", EndpointInvite(inviteID), nil, EndpointInvite(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// InviteAccept accepts an Invite to a Guild or Channel
// inviteID : The invite code (or maybe xkcdpass?)
func (s *Session) InviteAccept(inviteID string) (st *Invite, err error) {

	body, err := s.RequestWithBucketID("POST", EndpointInvite(inviteID), nil, EndpointInvite(""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Voice
// ------------------------------------------------------------------------------------------------

// VoiceRegions returns the voice server regions
func (s *Session) VoiceRegions() (st []*VoiceRegion, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointVoiceRegions, nil, EndpointVoiceRegions)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// VoiceICE returns the voice server ICE information
func (s *Session) VoiceICE() (st *VoiceICE, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointVoiceIce, nil, EndpointVoiceIce)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Websockets
// ------------------------------------------------------------------------------------------------

// Gateway returns the a websocket Gateway address
func (s *Session) Gateway() (gateway string, err error) {

	response, err := s.RequestWithBucketID("GET", EndpointGateway, nil, EndpointGateway)
	if err != nil {
		return
	}

	temp := struct {
		URL string `json:"url"`
	}{}

	err = unmarshal(response, &temp)
	if err != nil {
		return
	}

	gateway = temp.URL

	// Ensure the gateway always has a trailing slash.
	// MacOS will fail to connect if we add query params without a trailing slash on the base domain.
	if !strings.HasSuffix(gateway, "/") {
		gateway += "/"
	}

	return
}

// Functions specific to Webhooks

// WebhookCreate returns a new Webhook.
// channelID: The ID of a Channel.
// name     : The name of the webhook.
// avatar   : The avatar of the webhook.
func (s *Session) WebhookCreate(channelID, name, avatar string) (st *Webhook, err error) {

	data := struct {
		Name   string `json:"name"`
		Avatar string `json:"avatar,omitempty"`
	}{name, avatar}

	body, err := s.RequestWithBucketID("POST", EndpointChannelWebhooks(channelID), data, EndpointChannelWebhooks(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// ChannelWebhooks returns all webhooks for a given channel.
// channelID: The ID of a channel.
func (s *Session) ChannelWebhooks(channelID string) (st []*Webhook, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointChannelWebhooks(channelID), nil, EndpointChannelWebhooks(channelID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// GuildWebhooks returns all webhooks for a given guild.
// guildID: The ID of a Guild.
func (s *Session) GuildWebhooks(guildID string) (st []*Webhook, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointGuildWebhooks(guildID), nil, EndpointGuildWebhooks(guildID))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// Webhook returns a webhook for a given ID
// webhookID: The ID of a webhook.
func (s *Session) Webhook(webhookID string) (st *Webhook, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointWebhook(webhookID), nil, EndpointWebhooks)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// WebhookWithToken returns a webhook for a given ID
// webhookID: The ID of a webhook.
// token    : The auth token for the webhook.
func (s *Session) WebhookWithToken(webhookID, token string) (st *Webhook, err error) {

	body, err := s.RequestWithBucketID("GET", EndpointWebhookToken(webhookID, token), nil, EndpointWebhookToken("", ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// WebhookEdit updates an existing Webhook.
// webhookID: The ID of a webhook.
// name     : The name of the webhook.
// avatar   : The avatar of the webhook.
func (s *Session) WebhookEdit(webhookID, name, avatar string) (st *Role, err error) {

	data := struct {
		Name   string `json:"name,omitempty"`
		Avatar string `json:"avatar,omitempty"`
	}{name, avatar}

	body, err := s.RequestWithBucketID("PATCH", EndpointWebhook(webhookID), data, EndpointWebhooks)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// WebhookEditWithToken updates an existing Webhook with an auth token.
// webhookID: The ID of a webhook.
// token    : The auth token for the webhook.
// name     : The name of the webhook.
// avatar   : The avatar of the webhook.
func (s *Session) WebhookEditWithToken(webhookID, token, name, avatar string) (st *Role, err error) {

	data := struct {
		Name   string `json:"name,omitempty"`
		Avatar string `json:"avatar,omitempty"`
	}{name, avatar}

	body, err := s.RequestWithBucketID("PATCH", EndpointWebhookToken(webhookID, token), data, EndpointWebhookToken("", ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// WebhookDelete deletes a webhook for a given ID
// webhookID: The ID of a webhook.
func (s *Session) WebhookDelete(webhookID string) (st *Webhook, err error) {

	body, err := s.RequestWithBucketID("DELETE", EndpointWebhook(webhookID), nil, EndpointWebhooks)
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// WebhookDeleteWithToken deletes a webhook for a given ID with an auth token.
// webhookID: The ID of a webhook.
// token    : The auth token for the webhook.
func (s *Session) WebhookDeleteWithToken(webhookID, token string) (st *Webhook, err error) {

	body, err := s.RequestWithBucketID("DELETE", EndpointWebhookToken(webhookID, token), nil, EndpointWebhookToken("", ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)

	return
}

// WebhookExecute executes a webhook.
// webhookID: The ID of a webhook.
// token    : The auth token for the webhook
func (s *Session) WebhookExecute(webhookID, token string, wait bool, data *WebhookParams) (err error) {
	uri := EndpointWebhookToken(webhookID, token)

	if wait {
		uri += "?wait=true"
	}

	_, err = s.RequestWithBucketID("POST", uri, data, EndpointWebhookToken("", ""))

	return
}

// MessageReactionAdd creates an emoji reaction to a message.
// channelID : The channel ID.
// messageID : The message ID.
// emojiID   : Either the unicode emoji for the reaction, or a guild emoji identifier.
func (s *Session) MessageReactionAdd(channelID, messageID, emojiID string) error {

	_, err := s.RequestWithBucketID("PUT", EndpointMessageReaction(channelID, messageID, emojiID, "@me"), nil, EndpointMessageReaction(channelID, "", "", ""))

	return err
}

// MessageReactionRemove deletes an emoji reaction to a message.
// channelID : The channel ID.
// messageID : The message ID.
// emojiID   : Either the unicode emoji for the reaction, or a guild emoji identifier.
// userID	 : @me or ID of the user to delete the reaction for.
func (s *Session) MessageReactionRemove(channelID, messageID, emojiID, userID string) error {

	_, err := s.RequestWithBucketID("DELETE", EndpointMessageReaction(channelID, messageID, emojiID, userID), nil, EndpointMessageReaction(channelID, "", "", ""))

	return err
}

// MessageReactions gets all the users reactions for a specific emoji.
// channelID : The channel ID.
// messageID : The message ID.
// emojiID   : Either the unicode emoji for the reaction, or a guild emoji identifier.
// limit    : max number of users to return (max 100)
func (s *Session) MessageReactions(channelID, messageID, emojiID string, limit int) (st []*User, err error) {
	uri := EndpointMessageReactions(channelID, messageID, emojiID)

	v := url.Values{}

	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}

	if len(v) > 0 {
		uri = fmt.Sprintf("%s?%s", uri, v.Encode())
	}

	body, err := s.RequestWithBucketID("GET", uri, nil, EndpointMessageReaction(channelID, "", "", ""))
	if err != nil {
		return
	}

	err = unmarshal(body, &st)
	return
}

// ------------------------------------------------------------------------------------------------
// Functions specific to Discord Relationships (Friends list)
// ------------------------------------------------------------------------------------------------

// RelationshipsGet returns an array of all the relationships of the user.
func (s *Session) RelationshipsGet() (r []*Relationship, err error) {
	body, err := s.RequestWithBucketID("GET", EndpointRelationships(), nil, EndpointRelationships())
	if err != nil {
		return
	}

	err = unmarshal(body, &r)
	return
}

// relationshipCreate creates a new relationship. (I.e. send or accept a friend request, block a user.)
// relationshipType : 1 = friend, 2 = blocked, 3 = incoming friend req, 4 = sent friend req
func (s *Session) relationshipCreate(userID string, relationshipType int) (err error) {
	data := struct {
		Type int `json:"type"`
	}{relationshipType}

	_, err = s.RequestWithBucketID("PUT", EndpointRelationship(userID), data, EndpointRelationships())
	return
}

// RelationshipFriendRequestSend sends a friend request to a user.
// userID: ID of the user.
func (s *Session) RelationshipFriendRequestSend(userID string) (err error) {
	err = s.relationshipCreate(userID, 4)
	return
}

// RelationshipFriendRequestAccept accepts a friend request from a user.
// userID: ID of the user.
func (s *Session) RelationshipFriendRequestAccept(userID string) (err error) {
	err = s.relationshipCreate(userID, 1)
	return
}

// RelationshipUserBlock blocks a user.
// userID: ID of the user.
func (s *Session) RelationshipUserBlock(userID string) (err error) {
	err = s.relationshipCreate(userID, 2)
	return
}

// RelationshipDelete removes the relationship with a user.
// userID: ID of the user.
func (s *Session) RelationshipDelete(userID string) (err error) {
	_, err = s.RequestWithBucketID("DELETE", EndpointRelationship(userID), nil, EndpointRelationships())
	return
}

// RelationshipsMutualGet returns an array of all the users both @me and the given user is friends with.
// userID: ID of the user.
func (s *Session) RelationshipsMutualGet(userID string) (mf []*User, err error) {
	body, err := s.RequestWithBucketID("GET", EndpointRelationshipsMutual(userID), nil, EndpointRelationshipsMutual(userID))
	if err != nil {
		return
	}

	err = unmarshal(body, &mf)
	return
}
